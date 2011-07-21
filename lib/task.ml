type disco = {
  taskname : string;
  hostname : string;
  input_url : string;
  input_size : int;
  out_channel : label:int option -> out_channel;
  log : string -> unit;
  temp_dir : string;
}

module type TASK = sig
  type map_init
  val map_init : disco -> map_init
  val map : map_init -> disco -> in_channel -> unit
  val map_done : map_init -> disco -> unit

  type reduce_init
  val reduce_init : disco -> reduce_init
  val reduce : reduce_init -> disco -> in_channel -> unit
  val reduce_done : reduce_init -> disco -> unit
end
