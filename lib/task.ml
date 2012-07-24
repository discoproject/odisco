type disco = {
  jobname : string;
  jobfile : string;

  stage : string;
  group_label : int;
  group_node : string option;
  task_id : int;

  hostname : string;

  input_url : string;
  input_path : string;
  input_size : int;

  out_channel : label:int -> out_channel;
  log : string -> unit;

  temp_dir : string;
}

module type TASK = sig
  type init
  val task_init : disco -> init
  val task_process : init -> disco -> in_channel -> unit
  val task_done : init -> disco -> unit
end
