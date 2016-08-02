open Core.Std

open Dtc
open Bs_devkit.Core

let store_trade_in_db ?sync db ~ts ~price ~qty ~side =
  Tick.(LevelDB_ext.put_tick ?sync db @@ create ~ts ~side ~p:price ~v:qty ())
