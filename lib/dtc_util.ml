open Dtc.Dtc

module Trading = struct
  module Order = struct
    module Submit = struct
      let reject cs m k =
        let reject reason =
          Trading.Order.Update.write
            ~nb_msgs:1
            ~msg_number:1
            ~trade_account:m.Trading.Order.Submit.trade_account
            ~status:`Rejected
            ~reason:`New_order_rejected
            ~cli_ord_id:m.cli_ord_id
            ~symbol:m.Trading.Order.Submit.symbol
            ~exchange:m.exchange
            ~ord_type:m.ord_type
            ~buy_sell:m.buy_sell
            ~p1:m.p1
            ~p2:m.p2
            ~order_qty:m.qty
            ~tif:m.tif
            ~good_till_ts:m.good_till_ts
            ~info_text:reason
            ~free_form_text:m.text
            cs
        in
        Printf.ksprintf reject k

      let update ~status ~reason cs m k =
        let update info_text =
          Trading.Order.Update.write
            ~nb_msgs:1
            ~msg_number:1
            ~trade_account:m.Trading.Order.Submit.trade_account
            ~status
            ~reason
            ~cli_ord_id:m.Trading.Order.Submit.cli_ord_id
            ~ord_type:m.ord_type
            ~buy_sell:m.buy_sell
            ~open_or_close:m.open_close
            ~p1:m.p1
            ~p2:m.p2
            ~order_qty:m.qty
            ~tif:m.tif
            ~good_till_ts:m.good_till_ts
            ~info_text
            ~free_form_text:m.text
            cs
        in
        Printf.ksprintf update k
    end
  end
end
