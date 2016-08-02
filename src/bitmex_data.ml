(* Continuously pump data from BitMEX and serves it with DTC *)

open Core.Std
open Async.Std
open Log.Global
open Cohttp_async

open Bs_devkit.Core
open Bs_api.BMEX
open Dtc.Dtc

let use_testnet = ref false
let base_uri = ref @@ Uri.of_string "https://www.bitmex.com"
let datadir = ref "data"

let (//) = Filename.concat

let db_path symbol = !datadir // Uri.host_with_default !base_uri // symbol

let trades_exn ?start ?count () =
  let uri = Uri.with_path !base_uri "/api/v1/trade" in
  let uri = Option.value_map start ~default:uri
      ~f:(fun start -> Uri.add_query_param' uri ("start", string_of_int start))
  in
  let uri = Option.value_map count ~default:uri
      ~f:(fun count -> Uri.add_query_param' uri ("count", string_of_int count))
  in
  let uri = Uri.add_query_param uri ("columns", ["price"; "size"; "side"]) in
  debug "GET %s" @@ Uri.to_string uri;
  let process_trades body_str =
    match Yojson.Safe.from_string body_str with
    | `List trades -> List.map trades ~f:(Fn.compose Result.ok_or_failwith Trade.of_yojson)
    | json -> invalid_arg Yojson.Safe.(to_string json)
  in
  Monitor.try_with_or_error ~name:"trades_exn" (fun () ->
      Client.get uri >>=
      Rest.handle_rest_error ~name:"trades_exn"
    ) >>| function
  | Ok body_str -> process_trades body_str
  | Error err ->
    let err_str = Error.to_string_hum err in
    error "%s" err_str;
    []

let dbs = String.Table.create ()

let load_instruments_exn () =
  let iter_instrument json =
    let open RespObj in
    let t = of_json json in
    let key = string_exn t "symbol" in
    String.Table.add_exn dbs ~key
      ~data:(LevelDB.open_db @@ db_path key);
    info "Opened DB %s" (string_exn t "symbol")
  in
  let open Cohttp_async in
  let uri = Uri.with_path !base_uri "/api/v1/instrument/activeAndIndices" in
  debug "GET %s" @@ Uri.to_string uri;
  Monitor.try_with_or_error ~name:"instrument" (fun () ->
      Client.get uri >>=
      Rest.handle_rest_error ~name:"instrument"
    ) >>| function
  | Error err ->
    let err_str = Error.to_string_hum err in
    error "%s" err_str
  | Ok body_str -> begin
    match Yojson.Safe.from_string body_str with
    | `List instrs_json -> List.(iter instrs_json ~f:iter_instrument)
      | json -> invalid_arg Yojson.Safe.(to_string json)
    end

let store_trade_in_db db { Trade.timestamp; price; size; side } =
  let ts = Time_ns.of_string timestamp in
  let price = satoshis_int_of_float_exn price |> Int63.of_int in
  let qty = Int63.of_int64_exn size in
  let side = buy_sell_of_bmex side in
  Data_util.store_trade_in_db db ~ts ~price ~qty ~side

let bitmex_ws () =
  let buf = Bi_outbuf.create 4096 in
  let on_ws_msg json_str =
    let open Ws in
    let json = Yojson.Safe.from_string ~buf json_str in
    let start = int_of_string @@ In_channel.read_all @@ db_path "start" in
    match update_of_yojson json with
    | Error reason ->
      (match response_of_yojson json with
       | Error _ ->
         (match error_of_yojson json with
          | Error error ->
            debug "BitMEX sent malformed msg: %s" json_str
          | Ok { error=error_msg } ->
            error "BitMEX error: %s" error_msg
         )
       | Ok response ->
         info "%s" (show_response response)
      );
    | Ok { data } ->
      let open Trade in
      let nb_trades =
        List.fold_left data ~init:0
          ~f:(fun a t ->
              let t = Result.ok_or_failwith @@ Trade.of_yojson t in
              debug "%s %s %f %Ld" t.side t.symbol t.price t.size;
              let db = String.Table.find_or_add dbs t.symbol
                  ~default:(fun () ->
                      LevelDB.open_db @@ db_path t.symbol) in
              store_trade_in_db db t;
              succ a
            )
      in
      Out_channel.write_all
        (db_path "start") ~data:(string_of_int @@ start + nb_trades);
  in
  let ws = Ws.open_connection ~log:Lazy.(force log) ~testnet:!use_testnet ~md:false ~topics:["trade"] () in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> error "%s" @@ Exn.to_string exn)

let pump period =
  let start =
    try int_of_string @@ In_channel.read_all @@ db_path "start"
    with _ -> 0 in
  let rec loop start =
    try_with @@ trades_exn ~start ~count:500 >>= function
    | Error exn ->
      error "pump: %s" Exn.(to_string exn);
      if period < 600 then (* double the period and retry *)
        Clock_ns.after @@ Time_ns.Span.of_int_sec (2 * period) >>= fun () -> loop start
      else (* switch to WS *)
        bitmex_ws ()
    | Ok trades ->
      let open Trade in
      let nb_trades =
        List.fold_left ~init:0 trades
          ~f:(fun a t ->
              let db = String.Table.find_or_add dbs t.symbol
                  ~default:(fun () -> LevelDB.open_db @@ db_path t.symbol) in
              store_trade_in_db db t;
              succ a
            )
      in
      debug "%d new trades" nb_trades;
      match nb_trades with
      | 0 -> bitmex_ws ()
      | n ->
        Out_channel.write_all (db_path "start") ~data:(string_of_int @@ start + n);
        Clock_ns.after @@ Time_ns.Span.of_int_sec period >>= fun () -> loop (start + n)
  in
  loop start

(* A DTC Historical Price Server. *)

let tickserver port sockfile =
  let lr, lr_cs =
    let open Logon in
    let lr = Response.create
        ~server_name:"BitMEX Historical"
        ~result:`Success
        ~result_text:"Welcome to the BitMEX Historical Data Server"
        ~historical_price_data_supported:true
        ~market_depth_updates_best_bid_and_ask:true
        () in
    let cs = Cstruct.create Response.sizeof_cs in
    Response.to_cstruct cs lr;
    lr, cs in
  let process addr w cs scratchbuf =
    match msg_of_enum Cstruct.LE.(get_uint16 cs 2) with

    | Some LogonRequest ->
      let open Logon in
      let m = Request.read cs in
      debug "<-\n%s\n%!" (Request.show m);
      (* Send a logon_response message. *)
      debug "->\n%s\n%!" (Logon.Response.show lr);
      Writer.write_cstruct w lr_cs;
      `Continue

    | Some HistoricalPriceDataRequest ->
      let open HistoricalPriceData in
      let r_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Reject.sizeof_cs in
      let reject request_id k =
        Printf.ksprintf (
          fun str ->
            Reject.write r_cs request_id k;
            debug "-> HistoricalPriceData reject %s" str

        ) k;
        Writer.write_cstruct w r_cs in
      let open Request in
      let accept db m =
        (* SC sometimes uses negative timestamps. Correcting this. *)
        let start_ts =
          Int64.(if m.start_ts < 0L then 0L else m.start_ts * 1_000_000_000L) in
        let end_ts = Int64.(m.end_ts * 1_000_000_000L) in
        let record_interval = m.record_interval * 1_000_000_000 in
        let hdr = Header.create
            ~request_id:m.request_id ~record_ival:0 ~int_price_divisor:1e8 () in
        let hdr_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Header.sizeof_cs in
        Header.to_cstruct hdr_cs hdr;
        Writer.write_cstruct w hdr_cs;

        (* Now sending tick responses. *)

        let start_key = Bytes.create 8 in
        let end_key = Bytes.create 8 in
        Binary_packing.pack_signed_64_big_endian start_key 0 start_ts;
        Binary_packing.pack_signed_64_big_endian end_key 0 end_ts;
        info "Starting streaming %s-%s from %Ld to %Ld, interval = %d"
          m.symbol m.exchange start_ts end_ts record_interval;
        let nb_streamed = ref 0 in
        if m.record_interval = 0 then begin (* streaming tick responses *)
          let tick_cs = Cstruct.of_bigarray ~off:0 ~len:Tick.sizeof_cs scratchbuf in
          Tick.write tick_cs
            ~final:false ~request_id:m.request_id ~ts:0L ~p:0. ~v:0. ~side:`Unset;
          LevelDB.iter_from
            (fun ts data ->
               let ts = Binary_packing.unpack_signed_64_big_endian ts 0 in
               if end_ts > 0L && Int64.compare end_ts ts < 0 then false
               else begin
                 let ts = Time_ns.of_int63_ns_since_epoch @@ Int63.of_int64_exn ts in
                 let t = Dtc.Tick.Bytes.read' ~ts ~data () in
                 let ts = Time_ns.to_int_ns_since_epoch ts / 1_000_000_000 |> Float.of_int in
                 let p = Int63.to_float t.p /. 1e8 in
                 let v = Int63.to_float t.v in
                 (* Tick.write cs ~final:false ~request_id:m.request_id ~ts ~p ~v ~d; *)
                 Tick.set_cs_timestamp tick_cs (Int64.bits_of_float ts);
                 Tick.set_cs_side tick_cs @@ buy_or_sell_to_enum t.side;
                 Tick.set_cs_price tick_cs Int64.(bits_of_float p);
                 Tick.set_cs_volume tick_cs Int64.(bits_of_float v);
                 Writer.write_cstruct w tick_cs;
                 incr nb_streamed;
                 true
               end
            ) db start_key;
          Tick.write
            ~ts:0L ~p:0. ~v:0. ~side:`Unset ~request_id:m.request_id ~final:true tick_cs;
          Writer.write_cstruct w tick_cs;
          !nb_streamed, !nb_streamed
        end
        else begin (* streaming record responses *)
          let g = new Granulator.granulator
            ~request_id:m.request_id
            ~record_interval:Int64.(of_int record_interval)
            ~writer:w in
          LevelDB.iter_from
            (fun ts data ->
               let ts = Binary_packing.unpack_signed_64_big_endian ts 0 in
               if end_ts > 0L && Int64.compare end_ts ts < 0 then false
               else begin
                 let ts = Time_ns.of_int63_ns_since_epoch @@ Int63.of_int64_exn ts in
                 let t = Dtc.Tick.Bytes.read' ~ts ~data () in
                 let p = Int63.to_float t.p /. 1e8 in
                 let v = Int63.to_float t.v in
                 g#add_tick ts p v t.side;
                 true
               end
            ) db start_key;
          g#final
        end
      in
      let m = read cs in
      debug "<-\n%s\n%!" (show m);
      let open Option.Monad_infix in
      (match
         match !use_testnet, m.exchange with
         | true, "BMEX"
         | false, "BMEXT" -> None
         | _ -> String.Table.find dbs m.symbol >>| fun db -> accept db m
       with
       | None ->
         (error "No such symbol or DB for %s-%s" m.symbol m.exchange;
          reject m.request_id "No such symbol"
         )
       | Some (nb_streamed, nb_processed) ->
         info "Streamed %d/%d records from %s-%s"
           nb_streamed nb_processed m.symbol m.exchange
      );
      `Stop
    | Some _
    | None -> (* Unknown message, send LOGOFF/ do not reconnect. *)
      let open Logon in
      let logoff_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Logoff.sizeof_cs in
      Logoff.write logoff_cs ~reconnect:false "Unknown or unsupported message";
      Writer.write_cstruct w logoff_cs;
      `Stop
  in
  let handler addr r w =
    let addr_str = Socket.Address.to_string addr in
    debug "Incoming connection from %s" addr_str;
    let scratchbuf = Bigstring.create 1024 in
    let rec handle_chunk continue consumed buf ~pos ~len =
      let maybe_continue = function
        | `Continue -> return @@ `Consumed (consumed, `Need_unknown)
        | `Stop -> return @@ `Stop_consumed ((), consumed)
      in
      if len < 2 then maybe_continue continue
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        debug "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen;
        if len < msglen then maybe_continue continue
        else begin
          let msg_cs = Cstruct.of_bigarray buf ~off:pos ~len:msglen in
          let continue = process addr w msg_cs scratchbuf in
          handle_chunk continue (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    Reader.read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk `Continue 0) >>| fun _ ->
    debug "Client %s disconnected" addr_str
  in
  let on_handler_error addr exn =
    error "Client %s disconnected." Socket.Address.(to_string addr)
  in
  try_with (fun () -> Sys.remove sockfile) >>= fun _ ->
  let socket_server =
  Option.value_map port ~default:Deferred.unit ~f:(fun port ->
        Deferred.ignore Tcp.(Server.create ~on_handler_error:(`Call on_handler_error) (on_port port) handler);
      )
  in
  let tcp_server = Deferred.ignore Tcp.(Server.create ~on_handler_error:(`Call on_handler_error) (on_file sockfile) handler) in
  Deferred.all_unit [socket_server; tcp_server]

let main testnet period port sockfile daemon datadir' pidfile logfile loglevel =
  let sockfile = if testnet then add_suffix sockfile "_testnet" else sockfile in
  let pidfile = if testnet then add_suffix pidfile "_testnet" else pidfile in
  let logfile = if testnet then add_suffix logfile "_testnet" else logfile in
  let run () =
    load_instruments_exn () >>= fun () ->
    info "Data server starting";
    don't_wait_for @@ tickserver port sockfile;
    let rec loop_log_errors () =
      try_with (fun () -> pump period) >>= function
      | Error exn ->
        error "run: %s" Exn.(backtrace ());
        loop_log_errors ()
      | Ok _ -> assert false
    in
    loop_log_errors ()
  in

  (* begin initialization code *)
  if testnet then begin
    use_testnet := true;
    base_uri := Uri.of_string "https://testnet.bitmex.com";
  end;
  datadir := datadir';
  set_level (match loglevel with 2 -> `Info | 3 -> `Debug | _ -> `Error);
  if daemon then Daemon.daemonize ~cd:"." ();
  Signal.(handle terminating ~f:(fun _ ->
      info "Data server stopping";
      String.Table.iter dbs ~f:LevelDB.close;
      info "Saved %d dbs" @@ String.Table.length dbs;
      don't_wait_for @@ Shutdown.exit 0)
    );
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    set_output Log.Output.[stderr (); file `Text ~filename:logfile];
    run ();
  end;
  never_returns @@ Scheduler.go ()

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-testnet" no_arg ~doc:" Use testnet"
    +> flag "-period" (optional_with_default 5 int) ~doc:"seconds Period for HTTP GETs (5)"
    +> flag "-port" (optional int) ~doc:"int TCP port to use (not used)"
    +> flag "-sockfile" (optional_with_default "run/bitmex.sock" string) ~doc:"filename UNIX sock to use (run/bitmex.sock)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-datadir" (optional_with_default "data" string) ~doc:"path Where to store DBs (data)"
    +> flag "-pidfile" (optional_with_default "run/bitmex_data.pid" string) ~doc:"filename Path of the pid file (run/bitmex_data.pid)"
    +> flag "-logfile" (optional_with_default "log/bitmex_data.log" string) ~doc:"filename Path of the log file (log/bitmex_data.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
  in
  Command.basic ~summary:"BitMEX data aggregator" spec main

let () = Command.run command
