(* DTC to BitMEX simple bridge *)

open Core.Std
open Async.Std
open Cohttp_async

open Dtc.Dtc

open Bs_devkit.Core
open Bs_api.BMEX

(* Helper functions only useful here *)

let b64_of_uuid uuid_str =
  match Uuidm.of_string uuid_str with
  | None -> invalid_arg "Uuidm.of_string"
  | Some uuid -> Uuidm.to_bytes uuid |> B64.encode

let uuid_of_b64 b64 =
  B64.decode b64 |> Uuidm.of_bytes |> function
  | None -> invalid_arg "uuid_of_b64"
  | Some uuid -> Uuidm.to_string uuid

let bitmex_historical = ref ""

let use_testnet = ref false
let base_uri = ref @@ Uri.of_string "https://www.bitmex.com"
let exchange = ref "BMEX"
let my_topic = "bitsouk"

let log_bitmex = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_dtc = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]

type instr = {
  mutable instrObj: RespObj.t;
  secdef: SecurityDefinition.Response.t;
  mutable last_trade_price: (float [@default 0.]);
  mutable last_trade_size: (int64 [@default 0L]);
  mutable last_trade_ts: (Time_ns.t [@default Time_ns.epoch]);
  mutable last_quote_ts: (Time_ns.t [@default Time_ns.epoch]);
} [@@deriving create]

let instruments : instr String.Table.t = String.Table.create ()

type client = {
  addr: Socket.Address.Inet.t;
  addr_str: string;
  w: Writer.t;
  ws_r: Ws.update Pipe.Reader.t;
  ws_w: Ws.update Pipe.Writer.t;
  mutable ws_uuid: Uuid.t [@default Uuid.create ()];
  key: string;
  secret: Cstruct.t;
  position: RespObj.t I64S.Table.t; (* indexed by account, symbol *)
  margin: RespObj.t I64S.Table.t; (* indexed by account, currency *)
  order: RespObj.t Uuid.Table.t; (* indexed by orderID *)
  exec: RespObj.t Uuid.Table.t; (* indexed by execID *)
  mutable dropped: (int [@default 0]);
  subs: int String.Table.t;
  subs_depth: int String.Table.t;
  mutable current_parent: Trading.Order.Submit.t option;
  parents: string String.Table.t;
  stop_exec_inst: [`MarkPrice | `LastPrice];
} [@@deriving create]

let clients : client InetAddr.Table.t = InetAddr.Table.create ()

let set_parent_order client order =
  let open Trading.Order.Submit in
  client.current_parent <- Some order;
  String.Table.set client.parents order.cli_ord_id order.cli_ord_id

type book_entry = {
  price: float;
  size: int;
} [@@deriving create]

type books = {
  bids: book_entry Int.Table.t;
  asks: book_entry Int.Table.t;
} [@@deriving create]

let mapify_ob table =
  let fold_f ~key:_ ~data:{ price; size } map =
    Float.Map.update map price ~f:(function
        | Some size' -> size + size'
        | None -> size
      )
  in
  Int.Table.fold table ~init:Float.Map.empty ~f:fold_f

let orderbooks : books String.Table.t = String.Table.create ()
let quotes : Quote.t String.Table.t = String.Table.create ()

let heartbeat { addr_str; w; dropped } ival =
  let open Logon.Heartbeat in
  let cs = Cstruct.create sizeof_cs in
  let rec loop () =
    Clock_ns.after @@ Time_ns.Span.of_int_sec ival >>= fun () ->
    Log.debug log_dtc "-> %s HEARTBEAT" addr_str;
    write ~dropped_msgs:dropped cs;
    Writer.write_cstruct w cs;
    loop ()
  in
  Monitor.try_with_or_error ~name:"heatbeat" loop >>| function
  | Error err -> Log.error log_dtc "heartbeat: %s" @@ Error.to_string_hum err
  | Ok _ -> ()

let write_order_update ~nb_msgs ~msg_number cs e =
  let invalid_arg' execType ordStatus =
    invalid_arg Printf.(sprintf "write_order_update: execType=%s, ordStatus=%s" execType ordStatus)
  in
  let status_reason_of_execType_ordStatus e =
    let execType = RespObj.(string_exn e "execType") in
    let ordStatus = RespObj.(string_exn e "ordStatus") in
    if execType = ordStatus then
      match execType with
      | "New" -> `Open, `New_order_accepted
      | "PartiallyFilled" -> `Open, `Partially_filled
      | "Filled" -> `Filled, `Filled
      | "DoneForDay" -> `Open, `General_order_update
      | "Canceled" -> `Canceled, `Canceled
      | "PendingCancel" -> `Pending_cancel, `General_order_update
      | "Stopped" -> `Open, `General_order_update
      | "Rejected" -> `Rejected, `New_order_rejected
      | "PendingNew" -> `Pending_open, `General_order_update
      | "Expired" -> `Rejected, `New_order_rejected
      | _ -> invalid_arg' execType ordStatus
    else
      match execType, ordStatus with
      | "Restated", _ ->
        (match ordStatus with
         | "New" -> `Open, `General_order_update
         | "PartiallyFilled" -> `Open, `General_order_update
         | "Filled" -> `Filled, `General_order_update
         | "DoneForDay" -> `Open, `General_order_update
         | "Canceled" -> `Canceled, `General_order_update
         | "PendingCancel" -> `Pending_cancel, `General_order_update
         | "Stopped" -> `Open, `General_order_update
         | "Rejected" -> `Rejected, `General_order_update
         | "PendingNew" -> `Pending_open, `General_order_update
         | "Expired" -> `Rejected, `General_order_update
         | _ -> invalid_arg' execType ordStatus
        )
      | "Trade", "Filled" -> `Filled, `Filled
      | "Trade", "PartiallyFilled" -> `Filled, `Partially_filled
      | "Replaced", "New" -> `Open, `Cancel_replace_complete
      | "TriggeredOrActivatedBySystem", "New" -> `Open, `New_order_accepted
      | _ -> invalid_arg' execType ordStatus
  in
  match status_reason_of_execType_ordStatus e with
  | exception Invalid_argument msg ->
    Log.error log_bitmex "Not sending order update for %s" msg;
    false
  | status, reason ->
    let price = RespObj.(float_or_null_exn ~default:Float.max_finite_value e "price") in
    let stopPx = RespObj.(float_or_null_exn ~default:Float.max_finite_value e "stopPx") in
    let ord_type = ord_type_of_string RespObj.(string_exn e "ordType") in
    let p1, p2 = p1_p2_of_bitmex ~ord_type ~stopPx ~price in
    Trading.Order.Update.write
      ~nb_msgs
      ~msg_number
      ~symbol:RespObj.(string_exn e "symbol")
      ~exchange:!exchange
      ~cli_ord_id:RespObj.(string_exn e "clOrdID")
      ~srv_ord_id:RespObj.(string_exn e "orderID" |> b64_of_uuid)
      ~xch_ord_id:RespObj.(string_exn e "orderID" |> b64_of_uuid)
      ~ord_type
      ~status
      ~reason
      ~buy_sell:(buy_sell_of_bmex @@ RespObj.string_exn e "side")
      ?p1
      ?p2
      ~tif:(tif_of_string RespObj.(string_exn e "timeInForce"))
      ~order_qty:RespObj.(int64_exn e "orderQty" |> Int64.to_float)
      ~filled_qty:RespObj.(int64_exn e "cumQty" |> Int64.to_float)
      ~remaining_qty:RespObj.(int64_exn e "leavesQty" |> Int64.to_float)
      ?avg_fill_p:RespObj.(float e "avgPx")
      ?last_fill_p:RespObj.(float e "lastPx")
      ?last_fill_ts:RespObj.(string e "transactTime" |> Option.map ~f:(fun ts -> Time_ns.of_string ts |> Time_ns.to_int63_ns_since_epoch |> Int63.to_int64 |> fun v -> Int64.(v / 1_000_000_000L)))
      ?last_fill_qty:RespObj.(int64 e "lastQty" |> Option.map ~f:Int64.to_float)
      ~last_fill_exec_id:RespObj.(string_exn e "execID" |> b64_of_uuid)
      ~trade_account:RespObj.(int64_exn e "account" |> Int64.to_string)
      ~free_form_text:RespObj.(string_exn e "text")
      cs;
    true

let write_position_update ~unsolicited ~nb_msgs ~msg_number w cs p =
  Trading.Position.Update.write
    ~nb_msgs
    ~msg_number
    ~symbol:RespObj.(string_exn p "symbol")
    ~exchange:!exchange
    ~trade_account:RespObj.(int64_exn p "account" |> Int64.to_string)
    ~p:RespObj.(float_or_null_exn ~default:0. p "avgEntryPrice")
    ~v:RespObj.(int64_exn p "currentQty" |> Int64.to_float)
    ~unsolicited
    cs;
  Writer.write_cstruct w cs

let write_balance_update ~msg_number ~nb_msgs ~unsolicited cs m =
  let open Account.Balance.Update in
  write
    ~nb_msgs
    ~msg_number
    ~currency:"mXBT"
    ~cash_balance:RespObj.(Int64.(to_float @@ int64_exn m "walletBalance") /. 1e5)
    ~balance_available:RespObj.(Int64.(to_float @@ int64_exn m "availableMargin") /. 1e5)
    ~securities_value:RespObj.(Int64.(to_float @@ int64_exn m "marginBalance") /. 1e5)
    ~margin_requirement:RespObj.(Int64.(
        to_float (int64_exn m "initMargin" +
                  int64_exn m "maintMargin" +
                  int64_exn m "sessionMargin") /. 1e5))
    ~trade_account:RespObj.(int64_exn m "account" |> Int64.to_string)
    ~unsolicited
    cs

type subscribe_msg = Subscribe of client | Unsubscribe of Uuid.t * string
let client_ws_r, client_ws_w = Pipe.create ()

let client_ws ({ addr_str; w; ws_r; key; secret; order; margin; position; exec } as c) =
  let scratchbuf = Bigstring.create 4096 in
  let position_update_cs =
    Cstruct.of_bigarray scratchbuf ~len:Trading.Position.Update.sizeof_cs in
  let order_update_cs =
    Cstruct.of_bigarray scratchbuf ~len:Trading.Order.Update.sizeof_cs in
  let balance_update_cs =
    Cstruct.of_bigarray scratchbuf ~len:Account.Balance.Update.sizeof_cs in
  let order_partial_done = Ivar.create () in
  let margin_partial_done = Ivar.create () in
  let position_partial_done = Ivar.create () in
  let execution_partial_done = Ivar.create () in

  let process_orders action orders =
    let orders = List.map orders ~f:RespObj.of_json in
    List.iter orders ~f:(fun o ->
        let oid = RespObj.string_exn o "orderID" in
        let oid = Uuid.of_string oid in
        match action with
        | "delete" ->
          Uuid.Table.remove order oid;
          Log.debug log_bitmex "%s: order delete" addr_str
        | "insert" | "partial" ->
          Uuid.Table.set order ~key:oid ~data:o;
          Log.debug log_bitmex "%s: order insert/partial"
            addr_str
        | "update" ->
          if Ivar.is_full order_partial_done then begin
            let data = match Uuid.Table.find order oid with
              | None -> o
              | Some old_o -> RespObj.merge old_o o
            in
            Uuid.Table.set order ~key:oid ~data;
            Log.debug log_bitmex "%s: order update" addr_str
          end
        | _ -> invalid_arg @@ "process_orders: unknown action " ^ action
      );
    if action = "partial" then Ivar.fill_if_empty order_partial_done ()
  in
  let process_margins action margins =
    let margins = List.map margins ~f:RespObj.of_json in
    List.iteri margins ~f:(fun i m ->
        let a = RespObj.int64_exn m "account" in
        let c = RespObj.string_exn m "currency" in
        match action with
        | "delete" ->
          I64S.Table.remove margin (a, c);
          Log.debug log_bitmex "%s: margin delete" addr_str
        | "insert" | "partial" ->
          I64S.Table.set margin ~key:(a, c) ~data:m;
          Log.debug log_bitmex "%s: margin insert/partial"
            addr_str;
          write_balance_update ~unsolicited:true ~msg_number:1 ~nb_msgs:1
            balance_update_cs m;
          Writer.write_cstruct w balance_update_cs;
        | "update" ->
          if Ivar.is_full margin_partial_done then begin
            let m = match I64S.Table.find margin (a, c) with
              | None -> m
              | Some old_m -> RespObj.merge old_m m
            in
            I64S.Table.set margin ~key:(a, c) ~data:m;
            write_balance_update ~unsolicited:true ~msg_number:1 ~nb_msgs:1
              balance_update_cs m;
            Writer.write_cstruct w balance_update_cs;
            Log.debug log_bitmex "%s: margin update" addr_str
          end
        | _ -> invalid_arg @@ "process_margins: unknown action " ^ action
      );
    if action = "partial" then Ivar.fill_if_empty margin_partial_done ()
  in
  let process_positions action positions =
    let positions = List.map positions ~f:RespObj.of_json in
    List.iter positions ~f:(fun p ->
        let a = RespObj.int64_exn p "account" in
        let s = RespObj.string_exn p "symbol" in
        match action with
        | "delete" ->
          I64S.Table.remove position (a, s);
          Log.debug log_bitmex "%s: position delete" addr_str
        | "insert" | "partial" ->
          I64S.Table.set position ~key:(a, s) ~data:p;
          if RespObj.bool_exn p "isOpen" then begin
            write_position_update ~nb_msgs:1 ~msg_number:1 ~unsolicited:true
              w position_update_cs p;
            Log.debug log_bitmex "%s: position insert/partial" addr_str
          end
        | "update" ->
          if Ivar.is_full position_partial_done then begin
            let old_p, p = match I64S.Table.find position (a, s) with
              | None -> None, p
              | Some old_p -> Some old_p, RespObj.merge old_p p
            in
            I64S.Table.set position ~key:(a, s) ~data:p;
            match old_p with
            | Some old_p when RespObj.bool_exn old_p "isOpen" ->
              write_position_update ~nb_msgs:1 ~msg_number:1 ~unsolicited:true
                w position_update_cs p;
              Log.debug log_dtc "%s: position update %s" addr_str s
            | _ -> ()
          end
        | _ -> invalid_arg @@ "process_positions: unknown action " ^ action
      );
    if action = "partial" then Ivar.fill_if_empty position_partial_done ()
  in
  let process_execs action execs =
    let fold_f i e =
      let eid = RespObj.string_exn e "execID" in
      let eid = Uuid.of_string eid in
      begin match action with
        | "delete" ->
          Uuid.Table.remove exec eid;
          Log.debug log_bitmex "%s: exec delete" addr_str
        | "insert" | "partial" ->
          Uuid.Table.set exec ~key:eid ~data:e;
          Log.debug log_bitmex "%s: exec insert/partial"
            addr_str;
          if write_order_update ~nb_msgs:1 ~msg_number:1 order_update_cs e then
            Writer.write_cstruct w order_update_cs
        | "update" ->
          if Ivar.is_full execution_partial_done then begin
            let e = match Uuid.Table.find exec eid with
              | None -> e
              | Some old_e -> RespObj.merge old_e e
            in
            Uuid.Table.set exec ~key:eid ~data:e;
            Log.debug log_bitmex "%s: exec update" addr_str
          end
        | _ -> invalid_arg @@ "process_execs: unknown action " ^ action
      end;
      succ i
    in
    let execs = List.map execs ~f:RespObj.of_json in
    let nb_execs = List.fold_left execs ~init:0 ~f:fold_f in
    Log.debug log_dtc "%s -> %d execution update(s)" addr_str nb_execs;
    if action = "partial" then Ivar.fill_if_empty execution_partial_done ()
  in
  let on_update { Ws.table; action; data } =
    let open Ws in
    (* Erase scratchbuf by security. *)
    Bigstring.set_tail_padded_fixed_string scratchbuf ~padding:'\x00' ~pos:0 ~len:(Bigstring.length scratchbuf) "";
    match table, action, data with
      | "order", action, orders -> process_orders action orders
      | "margin", action, margins -> process_margins action margins
      | "position", action, positions -> process_positions action positions
      | "execution", action, execs -> process_execs action execs
      | table, _, _ -> Log.error log_bitmex "Unknown table %s" table
  in
  Pipe.write client_ws_w @@ Subscribe c >>= fun () ->
  don't_wait_for @@ Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws_r ~f:on_update)
    (fun exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn);
  Deferred.all_unit
    (List.map ~f:Ivar.read
       [execution_partial_done; order_partial_done;
        position_partial_done; margin_partial_done; ])

let process addr w msg_cs scratchbuf =
  let addr_str = Socket.Address.Inet.to_string addr in
  (* Erase scratchbuf by security. *)
  Bigstring.set_tail_padded_fixed_string
    scratchbuf ~padding:'\x00' ~pos:0 ~len:(Bigstring.length scratchbuf) "";
  let msg = msg_of_enum Cstruct.LE.(get_uint16 msg_cs 2) in
  let client = match msg with
    | None -> None
    | Some EncodingRequest -> None
    | Some LogonRequest -> None
    | Some msg -> begin
        match InetAddr.Table.find clients addr with
        | None ->
          Log.error log_dtc "msg type %s and found no client record" (show_msg msg);
          failwith "internal error: no client record"
        | Some client -> Some client
      end
  in
  match msg with
  | Some EncodingRequest ->
    let open Encoding in
    Log.debug log_dtc "<- ENCODING REQUEST";
    let response_cs = Cstruct.of_bigarray scratchbuf ~len:sizeof_cs in
    Response.write response_cs;
    Writer.write_cstruct w response_cs

  | Some LogonRequest ->
    let open Logon in
    let m = Request.read msg_cs in
    let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in
    Log.debug log_dtc "<- %s\n%s\n" addr_str (Request.show m);
    let create_client ~key ~secret ~stop_exec_inst =
      let ws_r, ws_w = Pipe.create () in
      create_client
        ~addr
        ~addr_str
        ~w
        ~ws_r
        ~ws_w
        ~key
        ~secret:Cstruct.(of_string secret)
        ~position:I64S.Table.(create ())
        ~margin:I64S.Table.(create ())
        ~order:Uuid.Table.(create ())
        ~exec:Uuid.Table.(create ())
        ~subs:String.Table.(create ())
        ~subs_depth:String.Table.(create ())
        ~parents:String.Table.(create ())
        ~stop_exec_inst
        ()
    in
    let accept client stop_exec_inst trading =
      let trading_supported, result_text =
        match trading with
        | Ok msg -> true, Printf.sprintf "Trading enabled: %s" msg
        | Error msg -> false, Printf.sprintf "Trading disabled: %s" msg
      in
      let resp =
        Response.create
          ~server_name:"BitMEX"
          ~result:`Success
          ~result_text
          ~security_definitions_supported:true
          ~market_data_supported:true
          ~historical_price_data_supported:true
          ~market_depth_supported:true
          ~market_depth_updates_best_bid_and_ask:true
          ~trading_supported
          ~ocr_supported:true
          ~oco_supported:true
          ~bracket_orders_supported:true
          ()
      in
      don't_wait_for @@ heartbeat client (Int32.to_int_exn m.Request.heartbeat_interval);
      Response.to_cstruct response_cs resp;
      Writer.write_cstruct w response_cs;
      Log.debug log_dtc "-> %s\n%s\n" addr_str (Response.show resp);
      let secdef_resp_cs =
        Cstruct.of_bigarray scratchbuf ~len:SecurityDefinition.Response.sizeof_cs
      in
      let instruments_len = String.Table.length instruments in
      let on_instrument ~key:_ ~data:{secdef; _} acc =
        let open SecurityDefinition.Response in
        let secdef =
          if acc = instruments_len - 1 then
            { secdef with final = true }
          else
            secdef
        in
        to_cstruct secdef_resp_cs secdef;
        Writer.write_cstruct w secdef_resp_cs;
        succ acc
      in
      ignore String.Table.(fold instruments ~init:0 ~f:on_instrument)
    in
    let setup_client_ws client =
      let ws_initialized = client_ws client in
      let ws_ok = choice ws_initialized
          (fun () ->
             Log.info log_dtc "BitMEX accepts API key for %s" m.username;
             Result.return "API key valid."
          )
      in
      let timeout = choice (Clock_ns.after @@ Time_ns.Span.of_int_sec 20)
          (fun () ->
             Log.info log_dtc "BitMEX rejects API key for %s" m.username;
             Result.fail "BitMEX rejects your API key."
          )
      in
      choose [ws_ok; timeout]
    in
    let stop_exec_inst = if Int32.bit_and m.integer_1 1l = 1l then `LastPrice else `MarkPrice in
    begin
      match m.username, m.password, m.general_text_data with
      | "", _, _ ->
        let client = create_client "" "" stop_exec_inst in
        InetAddr.Table.set clients ~key:addr ~data:client;
        accept client stop_exec_inst @@ Result.fail "No login provided, data only"
      | key, _, secret ->
        let client = create_client key secret stop_exec_inst in
        InetAddr.Table.set clients ~key:addr ~data:client;
        don't_wait_for (setup_client_ws client >>| accept client stop_exec_inst)
    end

  | Some Heartbeat ->
    Option.iter client ~f:(fun { addr_str } ->
        Log.debug log_dtc "<- %s HEARTBEAT" addr_str
      )

  | Some SecurityDefinitionForSymbolRequest ->
    let open SecurityDefinition in
    let m = Request.read msg_cs in
    let { addr_str; _ } = Option.value_exn client in
    Log.debug log_dtc "<- %s\n%s\n" addr_str (Request.show m);
    let reject_cs = Cstruct.of_bigarray scratchbuf ~len:Reject.sizeof_cs in
    let reject k =
      Printf.ksprintf
        (fun msg ->
           Log.info log_dtc "-> %s %s" addr_str msg;
           Reject.write reject_cs m.Request.id "%s" msg;
           Writer.write_cstruct w reject_cs
        ) k
    in
    if !exchange <> m.Request.exchange && not Instrument.(is_index m.Request.symbol) then
      reject "No such symbol %s-%s" m.symbol m.exchange
    else
      (match String.Table.find instruments m.Request.symbol with
       | None -> reject "No such symbol %s-%s" m.symbol m.exchange
       | Some { secdef; _ } ->
         let open Response in
         let secdef = { secdef with request_id = m.Request.id; final = true } in
         Log.debug log_dtc "-> %s\n%s\n" addr_str (show secdef);
         let secdef_resp_cs = Cstruct.of_bigarray scratchbuf ~len:sizeof_cs in
         Response.to_cstruct secdef_resp_cs secdef;
         Writer.write_cstruct w secdef_resp_cs
      )

  | Some MarketDataRequest ->
    let open MarketData in
    let m = Request.read msg_cs in
    Log.debug log_dtc "<- %s\n%s\n" addr_str (Request.show m);
    let { addr_str; subs; _ } = Option.value_exn client in
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let reject k =
      Printf.ksprintf
        (fun reason ->
           Reject.write r_cs m.Request.symbol_id "%s" reason;
           Log.debug log_dtc "-> %s Market Data Reject: %s" addr_str reason;
           Writer.write_cstruct w r_cs
        ) k
    in
    let accept { instrObj; last_trade_price; last_trade_size; last_trade_ts; last_quote_ts } =
      if m.Request.action = `Unsubscribe then String.Table.remove subs m.Request.symbol;
      if m.Request.action = `Subscribe then String.Table.set subs m.Request.symbol m.Request.symbol_id;
      let snap =
        if Instrument.is_index m.symbol then
          Snapshot.create
            ~symbol_id:m.symbol_id
            ?session_settlement_price:(RespObj.float instrObj "prevPrice24h")
            ?last_trade_p:(RespObj.float instrObj "lastPrice")
            ~last_trade_ts:(RespObj.string_exn instrObj "timestamp" |> Time_ns.of_string |> float_of_ts)
            ()
        else
          let open RespObj in
          match String.Table.find quotes m.symbol with
          | None ->
            Log.error log_dtc "market data request: no quote found for %s" m.symbol;
            failwith "no instrument found"
          | Some { Quote.bidPrice; bidSize; askPrice; askSize } ->
            Snapshot.create
              ~symbol_id:m.Request.symbol_id
              ~session_settlement_price:Option.(value ~default:Float.max_finite_value (float instrObj "indicativeSettlePrice"))
              ~session_h:Option.(value ~default:Float.max_finite_value @@ float instrObj "highPrice")
              ~session_l:Option.(value ~default:Float.max_finite_value @@ float instrObj "lowPrice")
              ~session_v:Option.(value_map (int64 instrObj "volume") ~default:Float.max_finite_value ~f:Int64.to_float)
              ~open_interest:Option.(value_map (int64 instrObj "openInterest") ~default:0xffffffffl ~f:Int64.to_int32_exn)
              ?bid:bidPrice
              ?bid_qty:Option.(map bidSize ~f:Float.of_int)
              ?ask:askPrice
              ?ask_qty:Option.(map askSize ~f:Float.of_int)
              ~last_trade_p:last_trade_price
              ~last_trade_v:Int64.(to_float last_trade_size)
              ~last_trade_ts:(float_of_ts last_trade_ts)
              ~bid_ask_ts:(float_of_ts last_quote_ts)
              ()
      in
      let snap_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Snapshot.sizeof_cs in
      Snapshot.to_cstruct snap_cs snap;
      Log.debug log_dtc "-> %s\n%s\n" addr_str (Snapshot.show snap);
      Writer.write_cstruct w snap_cs
    in
    if !exchange <> m.Request.exchange && not Instrument.(is_index m.Request.symbol) then
      reject "No such symbol %s-%s" m.symbol m.exchange
    else begin match String.Table.find instruments m.Request.symbol with
      | None -> reject "No such symbol %s-%s" m.symbol m.exchange
      | Some instr -> accept instr
    end

  | Some MarketDepthRequest ->
    let open MarketDepth in
    let m = Request.read msg_cs in
    let { addr_str; subs_depth; _ } = Option.value_exn client in
    Log.debug log_dtc "<- %s\n%s\n" addr_str (Request.show m);
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let reject k =
      Printf.ksprintf
        (fun msg ->
           Reject.write r_cs m.Request.symbol_id "%s" msg;
           Log.debug log_dtc "-> %s Market Depth Reject: %s" addr_str msg;
           Writer.write_cstruct w r_cs
        ) k
    in
    let accept { bids; asks } =
      if m.Request.action = `Unsubscribe then
        String.Table.remove subs_depth m.Request.symbol;
      if m.Request.action = `Subscribe then
        String.Table.set subs_depth m.Request.symbol m.Request.symbol_id;
      let snap_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Snapshot.sizeof_cs in
      let bids = mapify_ob bids in
      let asks = mapify_ob asks in
      if Float.Map.(is_empty bids && is_empty asks) then begin
        Snapshot.write snap_cs
          ~symbol_id:m.Request.symbol_id
          ~side:`Unset ~p:0. ~v:0. ~lvl:0 ~first:true ~last:true;
        Writer.write_cstruct w snap_cs
      end
      else begin
        Float.Map.fold_right bids ~init:1 ~f:(fun ~key:price ~data:size lvl ->
            Snapshot.write snap_cs
              ~symbol_id:m.Request.symbol_id
              ~side:`Bid ~p:price ~v:Float.(of_int size) ~lvl
              ~first:(lvl = 1) ~last:false;
            Writer.write_cstruct w snap_cs;
            succ lvl
          ) |> ignore;
        Float.Map.fold asks ~init:1 ~f:(fun ~key:price ~data:size lvl ->
            Snapshot.write snap_cs
              ~symbol_id:m.Request.symbol_id
              ~side:`Ask ~p:price ~v:Float.(of_int size) ~lvl
              ~first:(lvl = 1 && Float.Map.is_empty bids) ~last:false;
            Writer.write_cstruct w snap_cs;
            succ lvl
          ) |> ignore;
        Snapshot.write snap_cs
          ~symbol_id:m.Request.symbol_id
          ~side:`Unset ~p:0. ~v:0. ~lvl:0 ~first:false ~last:true;
        Writer.write_cstruct w snap_cs
      end
    in
    if Instrument.is_index m.symbol then
      reject "%s is an index" m.symbol
    else if !exchange <> m.Request.exchange then
      reject "No such symbol %s-%s" m.symbol m.exchange
    else if m.Request.action <> `Unsubscribe && not @@ String.Table.mem instruments m.Request.symbol then
      reject "No such symbol %s-%s" m.symbol m.exchange
    else begin match String.Table.find orderbooks m.symbol with
      | None ->
        Log.error log_dtc "market depth request: found no orderbooks for %s" m.symbol;
        failwith "market depth request: no orderbook found"
      | Some obs -> accept obs
    end

  | Some HistoricalPriceDataRequest ->
    let open HistoricalPriceData in
    let m = Request.read msg_cs in
    let { addr_str; _ } = Option.value_exn client in
    Log.debug log_dtc "<- %s\n%s\n" addr_str (Request.show m);
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let reject k =
      Printf.ksprintf (fun reason ->
          Reject.write r_cs m.Request.request_id "%s" reason;
          Log.debug log_dtc "-> %s HistoricalPriceData Reject\n" addr_str;
          Writer.write_cstruct w r_cs;
        ) k
    in
    let accept () =
      match String.Table.find instruments m.Request.symbol with
       | None ->
         reject "No such symbol %s-%s" m.symbol m.exchange;
         Deferred.unit
       | Some _ ->
         let f addr te_r te_w =
           Writer.write_cstruct te_w msg_cs;
           let r_pipe = Reader.pipe te_r in
           let w_pipe = Writer.pipe w in
           Pipe.transfer_id r_pipe w_pipe >>| fun () ->
           Log.debug log_dtc "-> %s <historical data>\n" addr_str
         in
         Monitor.try_with_or_error
           ~name:"historical_with_connection"
           (fun () -> Tcp.(with_connection (to_file !bitmex_historical) f)) >>| function
         | Error err -> reject "Historical data server unavailable"
         | Ok () -> ()
    in
    if !exchange <> m.Request.exchange
    && not Instrument.(is_index m.Request.symbol) then
      reject "No such symbol %s-%s" m.symbol m.exchange
    else don't_wait_for @@ accept ()

  | Some OpenOrdersRequest ->
    let open Trading.Order.Open in
    let m = Request.read msg_cs in
    let { addr_str; order; _ } = Option.value_exn client in
    Log.debug log_dtc "<- %s OpenOrdersRequest (%s)\n"
      addr_str (match m.Request.orders with `All -> "" | `One s -> s);
    let nb_open_orders, open_orders = Uuid.Table.fold order
        ~init:(0, [])
        ~f:(fun ~key:_ ~data ((nb_open_orders, os) as acc) ->
            match RespObj.(string_exn data "ordStatus") with
            | "New" | "PartiallyFilled" | "PendingCancel" ->
              (succ nb_open_orders, data :: os)
            | _ -> acc
          )
    in
    let open Trading.Order.Update in
    let update_cs = Cstruct.of_bigarray ~off:0 ~len:sizeof_cs scratchbuf in
    let send_order_update i data =
      let ord_type = ord_type_of_string RespObj.(string_exn data "ordType") in
      let price = RespObj.(float_or_null_exn ~default:Float.max_finite_value data "price") in
      let stopPx = RespObj.(float_or_null_exn ~default:Float.max_finite_value data "stopPx") in
      let p1, p2 = p1_p2_of_bitmex ~ord_type ~stopPx ~price in
      write
        ~nb_msgs:nb_open_orders
        ~msg_number:(succ i)
        ~status:`Open (* FIXME: PartiallyFilled ?? *)
        ~reason:`Open_orders_request_response
        ~request_id:m.Request.id
        ~symbol:RespObj.(string_exn data "symbol")
        ~exchange:!exchange
        ~cli_ord_id:RespObj.(string_exn data "clOrdID")
        ~srv_ord_id:RespObj.(string_exn data "orderID" |> b64_of_uuid)
        ~xch_ord_id:RespObj.(string_exn data "orderID" |> b64_of_uuid)
        ~ord_type:(ord_type_of_string RespObj.(string_exn data "ordType"))
        ~buy_sell:(buy_sell_of_bmex @@ RespObj.string_exn data "side")
        ?p1
        ?p2
        ~order_qty:RespObj.(int64_exn data "orderQty" |> Int64.to_float)
        ~filled_qty:RespObj.(int64_exn data "cumQty" |> Int64.to_float)
        ~remaining_qty:RespObj.(int64_exn data "leavesQty" |> Int64.to_float)
        ~tif:(tif_of_string RespObj.(string_exn data "timeInForce"))
        ~trade_account:RespObj.(int64_exn data "account" |> Int64.to_string)
        ~free_form_text:RespObj.(string_exn data "text")
        update_cs;
      Writer.write_cstruct w update_cs;
    in
    List.iteri open_orders ~f:send_order_update;
    if nb_open_orders = 0 then begin
      write ~nb_msgs:1 ~msg_number:1 ~request_id:m.Request.id
        ~reason:`Open_orders_request_response ~no_orders:true update_cs;
      Writer.write_cstruct w update_cs
    end;
    Log.debug log_dtc "-> %s %d orders" addr_str nb_open_orders;

  | Some CurrentPositionsRequest ->
    let open Trading.Position in
    let m = Request.read msg_cs in
    let update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in
    let { addr_str; position; _ } = Option.value_exn client in
    Log.debug log_dtc "<- %s Positions (%s)" addr_str m.Request.trade_account;
    let nb_open_positions, open_positions = I64S.Table.fold position
        ~init:(0, [])
        ~f:(fun ~key:_ ~data ((nb_open_ps, open_ps) as acc) ->
            if RespObj.bool_exn data "isOpen" then
              succ nb_open_ps, data :: open_ps
            else
              acc
          )
    in
    List.iteri open_positions
      ~f:(fun i data ->
          Update.write
            ~nb_msgs:nb_open_positions
            ~msg_number:(succ i)
            ~request_id:m.Request.id
            ~symbol:RespObj.(string_exn data "symbol")
            ~exchange:!exchange
            ~trade_account:RespObj.(int64_exn data "account" |> Int64.to_string)
            ~p:RespObj.(float_exn data "avgEntryPrice")
            ~v:RespObj.(int64_exn data "currentQty" |> Int64.to_float)
            ~unsolicited:false
            update_cs;
          Writer.write_cstruct w update_cs
        );
    if nb_open_positions = 0 then begin
      Update.write ~nb_msgs:1 ~msg_number:1 ~request_id:m.Request.id
        ~no_positions:true ~unsolicited:false update_cs;
      Writer.write_cstruct w update_cs
    end;
    Log.debug log_dtc "-> %s %d positions" addr_str nb_open_positions;

  | Some HistoricalOrderFillsRequest ->
    let open Trading.Order.Fills in
    let m = Request.read msg_cs in
    let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in
    let { addr_str; key; secret; _ } = Option.value_exn client in
    Log.debug log_dtc "<- %s\n%s\n" addr_str (Request.show m);
    let uri = Uri.with_path !base_uri "/api/v1/execution" in
    let uri = Uri.with_query' uri
        ["filter", Yojson.Safe.to_string @@
         `Assoc ((if m.Request.trade_account <> ""
                  then ["account", `String m.Request.trade_account]
                  else []) @
                 (if m.Request.srv_order_id <> ""
                  then ["orderID", `String m.Request.srv_order_id]
                  else []) @
                 ["ordStatus", `String "Filled"])
        ] in
    let process_body body_str =
      match Yojson.Safe.from_string body_str with
      | `List orders ->
        let nb_msgs = List.length orders in
        List.iteri orders
          ~f:(fun i o ->
              let o = RespObj.of_json o in
              Response.write
                ~nb_msgs
                ~msg_number:(succ i)
                ~request_id:m.Request.id
                ~symbol:RespObj.(string_exn o "symbol")
                ~exchange:!exchange
                ~srv_order_id:RespObj.(string_exn o "orderID" |> b64_of_uuid)
                ~p:RespObj.(float_exn o "avgPx")
                ~v:Float.(of_int64 RespObj.(int64_exn o "orderQty"))
                ~ts:Time.(of_string @@ RespObj.string_exn o "transactTime"
                          |> to_epoch |> Float.to_int64)
                ~open_close:`Unset
                ~buy_sell:(buy_sell_of_bmex @@ RespObj.string_exn o "side")
                ~exec_id:RespObj.(string_exn o "execID" |> b64_of_uuid)
                response_cs;
              Writer.write_cstruct w response_cs
            );
        Log.debug log_dtc "%s -> %d historical order fills" addr_str nb_msgs
      | #Yojson.Safe.json -> invalid_arg "bitmex historical order fills response"
    in
    let get_fills_and_reply_exn () =
      Monitor.try_with_or_error ~name:"execution" (fun () ->
           Client.get ~headers:(Rest.mk_headers ~key ~secret `GET uri) uri >>=
           Rest.handle_rest_error ~name:"execution"
        ) >>| function
      | Ok body_str -> process_body body_str
      | Error err ->
        (* TODO: reject on error *)
        Log.error log_bitmex "%s" @@ Error.to_string_hum err
    in don't_wait_for @@ eat_exn get_fills_and_reply_exn (* TODO: reject on error *)

  | Some TradeAccountsRequest ->
    let open Account.List in
    let m = Request.read msg_cs in
    let { addr_str; margin; _ } = Option.value_exn client in
    Log.debug log_dtc "<- %s TradeAccountsRequest" addr_str;
    let response_cs =
      Cstruct.of_bigarray ~off:0 ~len:Response.sizeof_cs scratchbuf in
    let account, _ = List.hd_exn @@ I64S.Table.keys margin in
    Response.write ~request_id:m.Request.id ~msg_number:1 ~nb_msgs:1
      ~trade_account:Int64.(to_string account) response_cs;
    Writer.write_cstruct w response_cs;
    Log.debug log_dtc "-> %s TradeAccountResponse: %Ld" addr_str account

  | Some AccountBalanceRequest ->
    let open Account.Balance in
    let m = Request.read msg_cs in
    let { addr_str; margin; _ } = Option.value_exn client in
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in
    let reject k =
      Printf.ksprintf (fun reason ->
          Reject.write r_cs m.Request.id "%s" reason;
          Writer.write_cstruct w r_cs;
          Log.debug log_dtc "-> %s AccountBalanceReject" addr_str;
        ) k
    in
    let write_no_balances request_id =
      Account.Balance.Update.write ~request_id
        ~nb_msgs:1 ~msg_number:1
        ~no_account_balance:true ~unsolicited:false update_cs;
      Writer.write_cstruct w update_cs;
      Log.debug log_dtc "-> %s AccountBalanceUpdate: no balance" addr_str
    in
    let update ~msg_number ~nb_msgs account_id obj =
      write_balance_update ~unsolicited:false ~msg_number ~nb_msgs update_cs obj;
      Writer.write_cstruct w update_cs;
      Log.debug log_dtc "-> %s AccountBalanceUpdate: %Ld" addr_str account_id
    in
    Log.debug log_dtc "<- %s AccountBalanceRequest (%s)"
      addr_str m.Request.trade_account;
    let nb_msgs = I64S.Table.length margin in
    if nb_msgs = 0 then
      write_no_balances m.id
    else if m.Request.trade_account = "" then
      ignore @@ I64S.Table.fold margin ~init:1
        ~f:(fun ~key:(account_id, currency) ~data msg_number ->
            update ~msg_number ~nb_msgs account_id data;
            succ msg_number
          )
    else begin
      match I64S.Table.find margin (Int64.of_string m.Request.trade_account, "XBt")
      with
      | Some obj -> update ~msg_number:1 ~nb_msgs:1 Int64.(of_string m.Request.trade_account) obj
      | None -> write_no_balances m.id
      | exception _ -> reject "Invalid trade account %s" m.Request.trade_account
    end

  | Some SubmitNewSingleOrder ->
    let module S = Trading.Order.Submit in
    let module U = Trading.Order.Update in
    let m = S.read msg_cs in
    let { addr_str; key; secret; current_parent; stop_exec_inst } as c = Option.value_exn client in
    let order_update_cs = Cstruct.of_bigarray ~off:0 ~len:U.sizeof_cs scratchbuf in
    Log.debug log_dtc "<- %s\n%s\n" addr_str (S.show m);
    let accept_exn () =
      let uri = Uri.with_path !base_uri "/api/v1/order" in
      let qty = match m.S.buy_sell with
        | `Unset
        | `Buy -> m.S.qty
        | `Sell -> Float.neg m.S.qty
      in
      let body =
        ["symbol", `String m.S.symbol;
         "orderQty", `Float qty;
         "timeInForce", `String (string_of_tif m.tif);
         "ordType", `String (string_of_ord_type m.S.ord_type);
         "clOrdID", `String m.S.cli_ord_id;
         "text", `String m.text;
        ]
        @ price_fields_of_dtc m.S.ord_type ~p1:m.S.p1 ~p2:m.S.p2
        @ execInst_of_dtc m.ord_type m.tif stop_exec_inst
      in
      let body_str = Yojson.Safe.to_string @@ `Assoc body in
      let body = Body.of_string body_str in
      Log.debug log_bitmex "-> %s" body_str;
      Monitor.try_with_or_error ~name:"submit" (fun () ->
          Client.post ~chunked:false ~body
            ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `POST uri)
            uri >>= Rest.handle_rest_error ~name:"submit"
        ) >>| function
      | Ok _body_str -> ()
      | Error err ->
        let err_str = Error.to_string_hum err in
        Dtc_util.Trading.Order.Submit.reject order_update_cs m "%s" err_str;
        Writer.write_cstruct w order_update_cs;
        Log.error log_bitmex "%s" err_str
    in
    if !exchange <> m.S.exchange then begin
      Dtc_util.Trading.Order.Submit.reject order_update_cs m "Unknown exchange";
      Writer.write_cstruct w order_update_cs;
    end
    else if List.mem [`Unset; `Good_till_date_time] m.S.tif then begin
      Dtc_util.Trading.Order.Submit.reject order_update_cs m "BitMEX does not support time in force %s" (show_time_in_force m.S.tif);
      Writer.write_cstruct w order_update_cs;
    end
    else if Option.is_some current_parent then begin
      c.current_parent <- None;
      Dtc_util.Trading.Order.Submit.reject order_update_cs Option.(value_exn current_parent) "Next received order was not an OCO";
      Writer.write_cstruct w order_update_cs;
      Dtc_util.Trading.Order.Submit.reject order_update_cs m "Previous received order was also a parent and the current order is not an OCO";
      Writer.write_cstruct w order_update_cs;
    end
    else if m.S.parent then begin
      set_parent_order c m;
      Dtc_util.Trading.Order.Submit.update
        ~status:`Pending_open ~reason:`General_order_update
        order_update_cs m "parent order %s stored" m.S.cli_ord_id;
      Writer.write_cstruct w order_update_cs;
      Log.debug log_dtc "Stored parent order %s" m.S.cli_ord_id
    end
    else
      let on_exn _ =
        Dtc_util.Trading.Order.Submit.update
          ~status:`Rejected ~reason:`New_order_rejected
          order_update_cs m
          "exception raised when trying to submit %s" m.S.cli_ord_id;
        Writer.write_cstruct w order_update_cs;
        Deferred.unit
      in
      don't_wait_for @@ eat_exn ~on_exn accept_exn

  | Some SubmitNewOCOOrder ->
    let module S = Trading.Order.SubmitOCO in
    let module S' = Trading.Order.Submit in
    let module U = Trading.Order.Update in
    let m = S.read msg_cs in
    let { addr_str; key; secret; current_parent; parents; stop_exec_inst } as c = Option.value_exn client in
    let order_update_cs = Cstruct.of_bigarray ~off:0 ~len:U.sizeof_cs scratchbuf in
    Log.debug log_dtc "<- %s\n%s\n" addr_str (S.show m);
    let reject cs m k =
      Printf.ksprintf (fun reason ->
          U.write
            ~nb_msgs:1
            ~msg_number:1
            ~trade_account:m.S.trade_account
            ~status:`Rejected
            ~reason:`New_order_rejected
            ~cli_ord_id:m.S.cli_ord_id_1
            ~symbol:m.S.symbol
            ~exchange:m.S.exchange
            ~ord_type:m.S.ord_type_1
            ~buy_sell:m.S.buy_sell_1
            ~p1:m.S.p1_1
            ~p2:m.S.p2_1
            ~order_qty:m.S.qty_1
            ~tif:m.S.tif
            ~good_till_ts:m.S.good_till_ts
            ~info_text:reason
            ~free_form_text:m.text
            cs;
          Writer.write_cstruct w cs;
          Log.debug log_dtc "-> %s Reject (%s)" addr_str reason;
          U.write
            ~nb_msgs:1
            ~msg_number:1
            ~trade_account:m.S.trade_account
            ~status:`Rejected
            ~reason:`New_order_rejected
            ~cli_ord_id:m.S.cli_ord_id_2
            ~symbol:m.S.symbol
            ~exchange:m.S.exchange
            ~ord_type:m.S.ord_type_2
            ~buy_sell:m.S.buy_sell_2
            ~p1:m.S.p1_2
            ~p2:m.S.p2_2
            ~order_qty:m.S.qty_2
            ~tif:m.S.tif
            ~good_till_ts:m.S.good_till_ts
            ~info_text:reason
            ~free_form_text:m.text
            cs;
          Writer.write_cstruct w cs;
          Log.debug log_dtc "-> %s Reject (%s)" addr_str reason) k
    in
    let accept_exn ?parent m =
      let uri = Uri.with_path !base_uri "/api/v1/order/bulk" in
      let qty_1 = match m.S.buy_sell_1 with
        | `Unset
        | `Buy -> m.S.qty_1
        | `Sell -> Float.neg m.S.qty_1
      in
      let qty_2 = match m.S.buy_sell_2 with
        | `Unset
        | `Buy -> m.S.qty_2
        | `Sell -> Float.neg m.S.qty_2
      in
      let orders = match parent with
        | None ->
          [
            ["symbol", `String m.S.symbol;
             "orderQty", `Float qty_1;
             "timeInForce", `String (string_of_tif m.tif);
             "ordType", `String (string_of_ord_type m.S.ord_type_1);
             "clOrdID", `String m.S.cli_ord_id_1;
             "contingencyType", `String "OneUpdatesTheOtherAbsolute";
             "clOrdLinkID", `String m.S.cli_ord_id_1;
             "text", `String m.text;
            ]
            @ price_fields_of_dtc m.S.ord_type_1 ~p1:m.S.p1_1 ~p2:m.S.p2_1
            @ execInst_of_dtc m.ord_type_1 m.tif stop_exec_inst;
            ["symbol", `String m.S.symbol;
             "orderQty", `Float qty_2;
             "timeInForce", `String (string_of_tif m.tif);
             "ordType", `String (string_of_ord_type m.S.ord_type_2);
             "clOrdID", `String m.S.cli_ord_id_2;
             "contingencyType", `String "OneUpdatesTheOtherAbsolute";
             "clOrdLinkID", `String m.S.cli_ord_id_1;
             "text", `String m.text;
            ]
            @ price_fields_of_dtc m.S.ord_type_2 ~p1:m.S.p1_2 ~p2:m.S.p2_2
            @ execInst_of_dtc m.ord_type_2 m.tif stop_exec_inst;
          ]
        | Some p ->
          let p_qty = match p.S'.buy_sell with
            | `Unset
            | `Buy -> p.S'.qty
            | `Sell -> Float.neg p.S'.qty
          in
          [
            ["symbol", `String p.S'.symbol;
             "orderQty", `Float p_qty;
             "timeInForce", `String (string_of_tif p.tif);
             "ordType", `String (string_of_ord_type p.S'.ord_type);
             "clOrdID", `String p.S'.cli_ord_id;
             "contingencyType", `String "OneTriggersTheOther";
             "clOrdLinkID", `String p.S'.cli_ord_id;
             "text", `String p.text;
            ]
            @ price_fields_of_dtc p.S'.ord_type ~p1:p.S'.p1 ~p2:p.S'.p2
            @ execInst_of_dtc p.ord_type p.tif stop_exec_inst;
            ["symbol", `String m.S.symbol;
             "orderQty", `Float qty_1;
             "timeInForce", `String (string_of_tif m.tif);
             "ordType", `String (string_of_ord_type m.S.ord_type_1);
             "clOrdID", `String m.S.cli_ord_id_1;
             "contingencyType", `String "OneUpdatesTheOtherAbsolute";
             "clOrdLinkID", `String p.S'.cli_ord_id;
             "text", `String m.text;
            ]
            @ price_fields_of_dtc m.S.ord_type_1 ~p1:m.S.p1_1 ~p2:m.S.p2_1
            @ execInst_of_dtc m.ord_type_1 m.tif stop_exec_inst;
            ["symbol", `String m.S.symbol;
             "orderQty", `Float qty_2;
             "timeInForce", `String (string_of_tif m.tif);
             "ordType", `String (string_of_ord_type m.S.ord_type_2);
             "clOrdID", `String m.S.cli_ord_id_2;
             "contingencyType", `String "OneUpdatesTheOtherAbsolute";
             "clOrdLinkID", `String p.S'.cli_ord_id;
             "text", `String m.text;
            ]
            @ price_fields_of_dtc m.S.ord_type_2 ~p1:m.S.p1_2 ~p2:m.S.p2_2
            @ execInst_of_dtc m.ord_type_2 m.tif stop_exec_inst
          ]
      in
      let body_str =
        `Assoc [ "orders", `List List.(map orders ~f:(fun o -> `Assoc o)) ] |> Yojson.Safe.to_string
      in
      let body = Body.of_string body_str in
      Log.debug log_bitmex "-> %s" body_str;
      Monitor.try_with_or_error ~name:"submit" (fun () ->
          Client.post ~chunked:false ~body
            ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `POST uri)
            uri >>= Rest.handle_rest_error ~name:"submit"
        ) >>| function
      | Ok _body_str ->
        if m.parent <> "" then begin
          String.Table.set parents m.cli_ord_id_1 m.parent;
          String.Table.set parents m.cli_ord_id_2 m.parent;
        end
        else begin
          String.Table.set parents m.cli_ord_id_1 m.cli_ord_id_1;
          String.Table.set parents m.cli_ord_id_2 m.cli_ord_id_1;
        end
      | Error err ->
        let err_str = Error.to_string_hum err in
        Option.iter parent ~f:(fun p ->
            Dtc_util.Trading.Order.Submit.reject order_update_cs p "%s" err_str;
            Writer.write_cstruct w order_update_cs
          );
        reject order_update_cs m "%s" err_str;
        Log.error log_bitmex "%s" err_str
    in
    if !exchange <> m.S.exchange then
      reject order_update_cs m "Unknown exchange"
    else if List.mem [`Unset; `Good_till_date_time] m.S.tif then
      reject order_update_cs m "BitMEX does not support time in force %s" (show_time_in_force m.S.tif)
    else if Option.is_none current_parent && m.S.parent <> "" then
      reject order_update_cs m "%s/%s is a child of %s but parent could not be found"
        m.cli_ord_id_1 m.cli_ord_id_2 m.S.parent
    else
      let on_exn ?p _ =
        Option.iter p ~f:(fun p ->
            Dtc_util.Trading.Order.Submit.reject order_update_cs p
              "exception raised when trying to submit cli=%s" p.S'.cli_ord_id;
            Writer.write_cstruct w order_update_cs
          );
        reject order_update_cs m
          "exception raised when trying to submit OCO cli=%s,%s" m.cli_ord_id_1 m.cli_ord_id_2;
        Deferred.unit
      in
      begin match current_parent with
        | None ->
          don't_wait_for @@ eat_exn ~on_exn (fun () -> accept_exn m)
        | Some p when p.S'.cli_ord_id = m.S.parent ->
          c.current_parent <- None;
          don't_wait_for @@
          eat_exn ~on_exn:(on_exn ~p) (fun () -> accept_exn ~parent:p m)
        | Some p ->
          c.current_parent <- None;
          (* Reject the parent *)
          Dtc_util.Trading.Order.Submit.reject order_update_cs p
            "parent order %s deleted (child %s/%s has parent %s)"
            p.cli_ord_id m.cli_ord_id_1 m.cli_ord_id_2 m.parent;
          Writer.write_cstruct w order_update_cs;
          (* Reject the child *)
          reject order_update_cs m
            "order %s/%s do not match stored parent %s (expected %s)"
            m.cli_ord_id_1 m.cli_ord_id_2 p.cli_ord_id m.parent
      end

  | Some CancelReplaceOrder ->
    let open Trading.Order in
    let reject m k =
      let order_update_cs = Cstruct.of_bigarray ~off:0
          ~len:Update.sizeof_cs scratchbuf
      in
      Printf.ksprintf (fun reason ->
          Update.write
            ~nb_msgs:1
            ~msg_number:1
            ~reason:`Cancel_replace_rejected
            ~cli_ord_id:m.Replace.cli_ord_id
            ~srv_ord_id:m.srv_ord_id
            ~ord_type:m.Replace.ord_type
            ~p1:m.Replace.p1
            ~p2:m.Replace.p2
            ~order_qty:m.Replace.qty
            ~tif:m.Replace.tif
            ~good_till_ts:m.Replace.good_till_ts
            ~info_text:reason
            order_update_cs;
          Writer.write_cstruct w order_update_cs;
          Log.debug log_dtc "-> %s Reject (%s)" addr_str reason) k
    in
    let m = Replace.read msg_cs in
    let { addr_str; key; secret; order } = Option.value_exn client in
    Log.debug log_dtc "<- %s\n%s\n" addr_str (Replace.show m);
    if m.Replace.ord_type <> `Unset then
      reject m "Modification of order type is not supported by BitMEX"
    else if m.Replace.tif <> `Unset then
      reject m "Modification of time in force is not supported by BitMEX"
    else
      let cancel_order_exn order =
        let orderID = RespObj.string_exn order "orderID" in
        let ord_type = RespObj.string_exn order "ordType" in
        let ord_type = ord_type_of_string ord_type in
        let uri = Uri.with_path !base_uri "/api/v1/order" in
        let p1 = if m.p1_set then Some m.p1 else None in
        let p2 = if m.p2_set then Some m.p2 else None in
        let body = ([Some ("orderID", `String orderID);
                     if m.Replace.qty <> 0. then Some ("orderQty", `Float m.Replace.qty) else None;
                    ] |> List.filter_opt)
                   @ price_fields_of_dtc ord_type ?p1 ?p2
        in
        let body_str = Yojson.Safe.to_string @@ `Assoc body in
        let body = Body.of_string body_str in
        Log.debug log_bitmex "-> %s\n%s" addr_str body_str;
        Monitor.try_with_or_error ~name:"cancel" (fun () ->
            Client.put
              ~chunked:false ~body
              ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `PUT uri)
              uri >>= Rest.handle_rest_error ~name:"cancel"
          ) >>| function
        | Ok body_str ->
          Log.debug log_bitmex "<- %s\n%s" addr_str body_str
        | Error err ->
          let err_str = Error.to_string_hum err in
          reject m "%s" err_str;
          Log.error log_bitmex "%s" err_str
      in
      let hex_srv_ord_id = uuid_of_b64 m.srv_ord_id in
      let on_exn _ =
        reject m "internal error when trying to cancel cli=%s srv=%s" m.cli_ord_id hex_srv_ord_id;
        Deferred.unit
      in
      begin
        match Uuid.Table.find order @@ Uuid.of_string hex_srv_ord_id with
        | None -> reject m "internal error: %s not found in db" m.srv_ord_id
        | Some o -> don't_wait_for @@ eat_exn ~on_exn ~log:log_bitmex (fun () -> cancel_order_exn o)
      end

  | Some CancelOrder ->
    let open Trading.Order in
    let reject m k =
      let order_update_cs = Cstruct.of_bigarray ~off:0
          ~len:Update.sizeof_cs scratchbuf
      in
      Printf.ksprintf (fun reason ->
          Update.write
            ~nb_msgs:1
            ~msg_number:1
            ~reason:`Cancel_rejected
            ~cli_ord_id:m.Cancel.cli_ord_id
            ~srv_ord_id:m.srv_ord_id
            ~info_text:reason
            order_update_cs;
          Writer.write_cstruct w order_update_cs;
          Log.debug log_dtc "-> %s Reject (%s)" addr_str reason) k
    in
    let m = Cancel.read msg_cs in
    let hex_srv_ord_id = uuid_of_b64 m.srv_ord_id in
    let { addr_str; key; secret; parents; } = Option.value_exn client in
    Log.debug log_dtc "<- %s Order Cancel cli=%s srv=%s" addr_str m.cli_ord_id hex_srv_ord_id;
    let cancel_order_exn = function
      | None -> begin
        let uri = Uri.with_path !base_uri "/api/v1/order" in
        let body_str = `Assoc ["orderID", `String hex_srv_ord_id] |> Yojson.Safe.to_string in
        let body = Body.of_string body_str in
        Client.delete
          ~chunked:false ~body
          ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `DELETE uri)
          uri >>= function (resp, body) ->
          Body.to_string body >>| fun body_str ->
          Log.debug log_bitmex "%s" body_str
        end
      | Some linkId -> begin
        let uri = Uri.with_path !base_uri "/api/v1/order/all" in
        let body_str = `Assoc ["filter", `Assoc ["clOrdLinkID", `String linkId]] |> Yojson.Safe.to_string in
        let body = Body.of_string body_str in
        Client.delete
          ~chunked:false ~body
          ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `DELETE uri)
          uri >>= function (resp, body) ->
          Body.to_string body >>| fun body_str ->
          Log.debug log_bitmex "%s" body_str
        end
    in
    let on_exn _ =
      reject m "exception raised while trying to cancel cli=%s srv=%s" m.cli_ord_id m.srv_ord_id;
      Deferred.unit
    in
    let parent = String.Table.find parents m.cli_ord_id in
    don't_wait_for @@ eat_exn ~on_exn ~log:log_bitmex
      (fun () -> cancel_order_exn parent)

  | Some _
  | None ->
    let buf = Buffer.create 128 in
    Cstruct.hexdump_to_buffer buf msg_cs;
    Log.error log_dtc "%s" @@ Buffer.contents buf

let dtcserver ~tls ~port () =
  let server_fun addr r w =
    (* So that process does not allocate all the time. *)
    let scratchbuf = Bigstring.create 1024 in
    let rec handle_chunk w consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        Log.debug log_dtc "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen;
        if len < msglen then return @@ `Consumed (consumed, `Need msglen)
        else begin
          let msg_cs = Cstruct.of_bigarray buf ~off:pos ~len:msglen in
          process addr w msg_cs scratchbuf;
          handle_chunk w (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    let on_client_io_error exn =
      Log.error log_dtc "on_client_io_error (%s): %s"
        Socket.Address.(to_string addr) Exn.(to_string exn)
    in
    let cleanup r w =
      Log.info log_dtc "client %s disconnected" Socket.Address.(to_string addr);
      let { ws_uuid } = InetAddr.Table.find_exn clients addr in
      let addr_str = InetAddr.sexp_of_t addr |> Sexp.to_string in
      InetAddr.Table.remove clients addr;
      Deferred.all_unit [Pipe.write client_ws_w @@ Unsubscribe (ws_uuid, addr_str); Writer.close w; Reader.close r]
    in
    Deferred.ignore @@ Monitor.protect
      ~name:"server_fun"
      ~finally:(fun () -> cleanup r w)
      (fun () ->
         Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_client_io_error;
         Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk w 0))
      )
  in
  let on_handler_error_f addr exn =
    Log.error log_dtc "on_handler_error (%s): %s"
      Socket.Address.(to_string addr) Exn.(backtrace ())
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    tls (Tcp.on_port port) server_fun

let send_instr_update_msgs w buf_cs instr symbol_id =
  let open RespObj in
  Option.iter (int64 instr "volume")
    ~f:(fun v ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`Volume ~symbol_id ~data:Int64.(to_float v) buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (float instr "lowPrice")
    ~f:(fun p ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`Low ~symbol_id ~data:p buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (float instr "highPrice")
    ~f:(fun p ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`High ~symbol_id ~data:p buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (int64 instr "openInterest")
    ~f:(fun p ->
        let open MarketData.UpdateOpenInterest in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~symbol_id ~open_interest:Int64.(to_int32_exn p) buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (float instr "prevClosePrice")
    ~f:(fun p ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`Open ~symbol_id ~data:p buf_cs;
        Writer.write_cstruct w buf_cs
      )

let delete_instr instrObj =
  let instrObj = RespObj.of_json instrObj in
  Log.info log_bitmex "deleted instrument %s" RespObj.(string_exn instrObj "symbol")

let insert_instr instrObj =
  let instrObj = RespObj.of_json instrObj in
  let key = RespObj.string_exn instrObj "symbol" in
  let secdef = Instrument.to_secdef ~testnet:!use_testnet instrObj in
  String.Table.set instruments key (create_instr ~instrObj ~secdef ());
  String.Table.set orderbooks ~key
    ~data:(create_books
             ~bids:Int.Table.(create ())
             ~asks:Int.Table.(create ()) ());
  Log.info log_bitmex "inserted instrument %s" key

let update_instr buf_cs instrObj =
  let instrObj = RespObj.of_json instrObj in
  let symbol = RespObj.string_exn instrObj "symbol" in
  match String.Table.find instruments symbol with
  | None ->
    Log.error log_bitmex "update_instr: unable to find %s" symbol;
  | Some instr ->
    instr.instrObj <- RespObj.merge instr.instrObj instrObj;
    Log.debug log_bitmex "updated instrument %s" symbol;
    (* Send messages to subscribed clients according to the type of
         update. *)
    let on_client { addr; addr_str; w; subs; _ } =
      let on_symbol_id symbol_id =
        send_instr_update_msgs w buf_cs instrObj symbol_id;
        Log.debug log_dtc "-> %s instrument %s" addr_str symbol
      in
      Option.iter String.Table.(find subs symbol) ~f:on_symbol_id
    in
    InetAddr.Table.iter clients ~f:on_client

let update_depths update_cs action { OrderBook.L2.symbol; id; side; size; price } =
  (* find_exn cannot raise here *)
  let { bids; asks } =  String.Table.find_exn orderbooks symbol in
  let table, bidask = match side with
    | "Buy" -> bids, `Bid
    | "Sell" -> asks, `Ask
    | _ -> invalid_arg "side_of_string"
  in
  let price = match price with
    | Some p -> Some p
    | None ->
      begin match Int.Table.find table id with
        | Some { price } -> Some price
        | None -> None
      end
  in
  let size = match size with
    | Some s -> Some s
    | None ->
      begin match Int.Table.find table id with
        | Some { size } -> Some size
        | None -> None
      end
  in
  match price, size with
  | Some price, Some size ->
    begin match action with
      | OB.Partial | Insert | Update -> Int.Table.set table id (create_book_entry ~size ~price ())
      | Delete -> Int.Table.remove table id
    end;
    let on_client { addr; addr_str; w; subs; subs_depth; _} =
      let on_symbol_id symbol_id =
        Log.debug log_dtc "-> %s depth %s %s %s %f %d"
          addr_str (OB.sexp_of_action action |> Sexp.to_string) symbol side price size;
        MarketDepth.Update.write
          ~op:(match action with Partial | Insert | Update -> `Insert_update | Delete -> `Delete)
          ~p:price
          ~v:(Float.of_int size)
          ~symbol_id
          ~side:bidask
          update_cs;
        Writer.write_cstruct w update_cs
      in
      Option.iter String.Table.(find subs_depth symbol) ~f:on_symbol_id
    in
    InetAddr.Table.iter clients ~f:on_client
  | _ ->
    Log.info log_bitmex "update_depth: received update before snapshot, ignoring"

let update_quote update_cs q =
  let old_q = String.Table.find_or_add quotes q.Quote.symbol ~default:(fun () -> q) in
  let merged_q = Quote.merge old_q q in
  let bidPrice = Option.value ~default:Float.max_finite_value merged_q.bidPrice in
  let bidSize = Option.value ~default:0 merged_q.bidSize in
  let askPrice = Option.value ~default:Float.max_finite_value merged_q.askPrice in
  let askSize = Option.value ~default:0 merged_q.askSize in
  String.Table.set quotes ~key:q.symbol ~data:merged_q;
  let on_client { addr; addr_str; w; subs; subs_depth; _} =
    let on_symbol_id symbol_id =
      Log.debug log_dtc "-> %s bidask %s %f %d %f %d"
        addr_str q.symbol bidPrice bidSize askPrice askSize;
      MarketData.UpdateBidAsk.write
        ~symbol_id
        ~bid:bidPrice
        ~bid_qty:Float.(of_int bidSize)
        ~ask:askPrice
        ~ask_qty:Float.(of_int askSize)
        ~ts:(int32_of_ts Time_ns.(of_string merged_q.timestamp))
        update_cs;
      Writer.write_cstruct w update_cs
    in
    match String.Table.(find subs q.symbol, find subs_depth q.symbol) with
    | Some id, None -> on_symbol_id id
    | _ -> ()
  in
  InetAddr.Table.iter clients ~f:on_client

let update_trade cs t =
  let open Trade in
  Log.debug log_bitmex "trade %s %s %f %Ld" t.symbol t.side t.price t.size;
  match String.Table.find instruments t.symbol with
  | None ->
    Log.error log_bitmex "update_trade: found no instrument for %s" t.symbol
  | Some instr ->
    instr.last_trade_price <- t.price;
    instr.last_trade_size <- t.size;
    instr.last_trade_ts <- Time_ns.of_string t.timestamp;
    (* Send trade updates to subscribers. *)
    let on_client { addr; addr_str; w; subs; _} =
      let on_symbol_id symbol_id =
        MarketData.UpdateTrade.write
          ~symbol_id
          ~side:(match t.side with "Buy" -> `Ask | _ -> `Bid)
          ~p:t.price
          ~v:Int64.(to_float t.size)
          ~ts:Time.(of_string t.timestamp |> to_float) cs;
        Writer.write_cstruct w cs;
        Log.debug log_dtc "-> %s trade %s %s %f %Ld"
          addr_str t.symbol t.side t.price t.size
      in
      Option.iter String.Table.(find subs t.symbol) ~f:on_symbol_id
    in
    InetAddr.Table.iter clients ~f:on_client

let bitmex_ws ~instrs_initialized ~orderbook_initialized ~quotes_initialized =
  let open Ws in
  let bi_outbuf = Bi_outbuf.create 4096 in
  let buf_cs = Bigstring.create 4096 in
  let trade_update_cs = Cstruct.of_bigarray ~len:MarketData.UpdateTrade.sizeof_cs buf_cs in
  let depth_update_cs = Cstruct.create MarketDepth.Update.sizeof_cs in
  let bidask_update_cs = Cstruct.create MarketData.UpdateBidAsk.sizeof_cs in
  let on_update update =
    let action = update_action_of_string update.action in
    match action, update.table, update.data with
    | Update, "instrument", instrs ->
      if Ivar.is_full instrs_initialized then List.iter instrs ~f:(update_instr buf_cs)
    | Delete, "instrument", instrs ->
      if Ivar.is_full instrs_initialized then List.iter instrs ~f:delete_instr
    | _, "instrument", instrs ->
      List.iter instrs ~f:insert_instr;
      Ivar.fill_if_empty instrs_initialized ()
    | _, "orderBookL2", depths ->
      let filter_f json =
        match OrderBook.L2.of_yojson json with
        | Ok u -> Some u
        | Error reason ->
          Log.error log_bitmex "%s: %s (%s)"
            reason Yojson.Safe.(to_string json) update.action;
          None
      in
      let depths = List.filter_map depths ~f:filter_f in
      don't_wait_for begin
        Ivar.read instrs_initialized >>| fun () ->
        List.iter depths ~f:(update_depths depth_update_cs action);
        Ivar.fill_if_empty orderbook_initialized ()
      end
    | _, "trade", trades ->
      let open Trade in
      let iter_f t = match Trade.of_yojson t with
        | Error msg ->
          Log.error log_bitmex "%s" msg
        | Ok t -> update_trade trade_update_cs t
      in
      don't_wait_for begin
        Ivar.read instrs_initialized >>| fun () ->
        List.iter trades ~f:iter_f
      end
    | _, "quote", quotes ->
      let filter_f json =
        match Quote.of_yojson json with
        | Ok q -> Some q
        | Error reason ->
          Log.error log_bitmex "%s: %s (%s)"
            reason Yojson.Safe.(to_string json) update.action;
          None
      in
      let quotes = List.filter_map quotes ~f:filter_f in
      List.iter quotes ~f:(update_quote bidask_update_cs);
      Ivar.fill_if_empty quotes_initialized ()
    | _, table, json ->
      Log.error log_bitmex "Unknown/ignored BitMEX DB table %s or wrong json %s"
        table Yojson.Safe.(to_string @@ `List json)
  in
  let to_ws, to_ws_w = Pipe.create () in
  let bitmex_topics = ["instrument"; "quote"; "orderBookL2"; "trade"] in
  let clients_topics = ["order"; "execution"; "position"; "margin"] in
  let subscribe_topics ~id ~topic ~topics =
    let payload =
      create_request
        ~op:"subscribe"
        ~args:(List.map topics ~f:(fun t -> `String t))
        ()
    in
    MD.message id topic (request_to_yojson payload)
  in
  let ws = open_connection
      ~to_ws
      ~log:log_bitmex
      ~testnet:!use_testnet
      ~md:true
      ~topics:[] ()
  in
  let my_uuid = Uuid.create () in
  Pipe.write_without_pushback to_ws_w @@ MD.to_yojson @@ MD.subscribe ~id:my_uuid ~topic:my_topic;
  let subscribe_client ({ addr; key; secret } as c) =
    let id = Uuid.create () in
    let topic = InetAddr.sexp_of_t addr |> Sexp.to_string_mach in
    c.ws_uuid <- id;
    Pipe.write to_ws_w @@ MD.to_yojson @@ MD.subscribe ~id ~topic
  in
  Deferred.List.iter (InetAddr.Table.to_alist clients) ~how:`Sequential ~f:(fun (_addr, c) -> subscribe_client c) >>= fun () ->
  don't_wait_for begin
    Pipe.iter client_ws_r ~f:begin function
      | Subscribe c -> subscribe_client c
      | Unsubscribe (id, topic) -> Pipe.write to_ws_w @@ MD.to_yojson @@ MD.unsubscribe ~id ~topic
    end
  end;
  let on_ws_msg json_str =
    let msg = Yojson.Safe.from_string ~buf:bi_outbuf json_str in
    match MD.of_yojson msg |> Result.ok_or_failwith with
    | { MD.typ; id; topic; payload = Some payload } -> begin
        match Ws.msg_of_yojson payload, topic = my_topic with

        (* Server *)
        | Welcome, true ->
          Pipe.write_without_pushback to_ws_w @@ MD.to_yojson @@ subscribe_topics my_uuid my_topic bitmex_topics
        | Error msg, true ->
          Log.error log_bitmex "BitMEX: error %s" msg
        | Ok { request = { op = "subscribe" }; subscribe; success }, true ->
          Log.info log_bitmex "BitMEX: subscribed to %s: %b" subscribe success
        | Ok { success }, true ->
          Log.error log_bitmex "BitMEX: unexpected response %s" (Yojson.Safe.to_string payload)
        | Update update, true -> on_update update

        (* Clients *)
        | Welcome, false ->
          let addr = Fn.compose InetAddr.Blocking_sexp.t_of_sexp Sexp.of_string topic in
          Option.iter (InetAddr.Table.find clients addr) ~f:(fun { key; secret } ->
              Pipe.write_without_pushback to_ws_w @@ MD.to_yojson @@ MD.auth ~id ~topic ~key ~secret
            )
        | Error msg, false ->
          Log.error log_bitmex "%s: error %s" topic msg
        | Ok { request = { op = "authKey"}; success}, false ->
          Pipe.write_without_pushback to_ws_w @@ MD.to_yojson @@ subscribe_topics ~id ~topic ~topics:clients_topics
        | Ok { request = { op = "subscribe"}; subscribe; success}, false ->
          Log.info log_bitmex "%s: subscribed to %s: %b" topic subscribe success
        | Ok { success }, false ->
          Log.error log_bitmex "%s: unexpected response %s" topic (Yojson.Safe.to_string payload)
        | Update update, false ->
          let addr = Sexp.of_string topic |> InetAddr.Blocking_sexp.t_of_sexp in
          match InetAddr.Table.find clients addr with
          | None -> if typ <> Unsubscribe then Pipe.write_without_pushback client_ws_w (Unsubscribe (id, topic))
          | Some { ws_w } -> Pipe.write_without_pushback ws_w update
      end
    | _ -> ()
  in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn)

let main tls testnet port daemon sockfile pidfile logfile loglevel ll_dtc ll_bitmex crt_path key_path =
  let sockfile = if testnet then add_suffix sockfile "_testnet" else sockfile in
  let pidfile = if testnet then add_suffix pidfile "_testnet" else pidfile in
  let logfile = if testnet then add_suffix logfile "_testnet" else logfile in
  let tls = if tls then `OpenSSL (`Crt_file_path crt_path, `Key_file_path key_path) else `TCP in
  let run () =
    let instrs_initialized = Ivar.create () in
    let orderbook_initialized = Ivar.create () in
    let quotes_initialized = Ivar.create () in
    let bitmex_th = bitmex_ws ~instrs_initialized ~orderbook_initialized ~quotes_initialized in
    Deferred.List.iter ~how:`Parallel ~f:Ivar.read
      [instrs_initialized; orderbook_initialized; quotes_initialized] >>= fun () ->
    dtcserver ~tls ~port () >>= fun dtc_server ->
    Deferred.all_unit [Tcp.Server.close_finished dtc_server; bitmex_th]
  in
  let rec loop_log_errors () =
    Monitor.try_with_or_error ~name:"loop_log_errors" run >>= function
    | Ok _ -> assert false
    | Error exn ->
      Log.error log_dtc "run: %s" @@ Error.to_string_hum exn;
      loop_log_errors ()
  in

  (* start initilization code *)
  if testnet then begin
    use_testnet := testnet;
    base_uri := Uri.of_string "https://testnet.bitmex.com";
    exchange := "BMEXT"
  end;
  bitmex_historical := sockfile;

  Log.set_level log_dtc @@ loglevel_of_int @@ max loglevel ll_dtc;
  Log.set_level log_bitmex @@ loglevel_of_int @@ max loglevel ll_bitmex;

  if daemon then Daemon.daemonize ~cd:"." ();
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    Writer.open_file ~append:true logfile >>= fun log_writer ->
    Log.(set_output log_dtc Output.[stderr (); writer `Text log_writer]);
    Log.(set_output log_bitmex Output.[stderr (); writer `Text log_writer]);
    loop_log_errors ()
  end;
  never_returns @@ Scheduler.go ()

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-tls" no_arg ~doc:" Use TLS"
    +> flag "-testnet" no_arg ~doc:" Use testnet"
    +> flag "-port" (optional_with_default 5567 int) ~doc:"int TCP port to use (5567)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-sockfile" (optional_with_default "run/bitmex.sock" string) ~doc:"filename UNIX sock to use (run/bitmex.sock)"
    +> flag "-pidfile" (optional_with_default "run/bitmex.pid" string) ~doc:"filename Path of the pid file (run/bitmex.pid)"
    +> flag "-logfile" (optional_with_default "log/bitmex.log" string) ~doc:"filename Path of the log file (log/bitmex.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel for DTC"
    +> flag "-loglevel-dtc" (optional_with_default 1 int) ~doc:"1-3 loglevel for DTC"
    +> flag "-loglevel-bitmex" (optional_with_default 1 int) ~doc:"1-3 loglevel for BitMEX"
    +> flag "-crt-file" (optional_with_default "ssl/bitsouk.com.crt" string) ~doc:"filename crt file to use (TLS)"
    +> flag "-key-file" (optional_with_default "ssl/bitsouk.com.key" string) ~doc:"filename key file to use (TLS)"
  in
  Command.basic ~summary:"BitMEX bridge" spec main

let () = Command.run command
