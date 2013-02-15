(** The Disco worker protocol. *)

(** Messages from Disco to worker. *)

type taskinfo = {
  (* info from protocol *)
  task_jobname : string;
  task_jobfile : string;

  task_stage : Pipeline.stage;
  task_grouping : Pipeline.grouping;
  task_group_label : Pipeline.label;
  task_group_node : string option;
  task_id : int;

  task_host : string;

  task_master : string;
  task_disco_port : int;
  task_put_port : int;
  task_disco_root : string;
  task_ddfs_root : string;

  (* runtime state *)
  task_rootpath : string;
}

type scheme =
  | Dir
  | Disco
  | File
  | Raw
  | Http
  | Other of string

val string_of_scheme : scheme -> string

type task_input_status =
  | Task_input_more
  | Task_input_done

type input_status =
  | Input_ok
  | Input_failed

type input_label =
  | Input_label_all
  | Input_label of Pipeline.label

type input_id = int
type replica_id = int
type replica = replica_id * Uri.t
type input = input_id * input_status * input_label * replica list

type master_msg =
  | M_ok
  | M_die
  | M_taskinfo of taskinfo
  | M_task_input of task_input_status * input list
  | M_retry of replica list
  | M_fail

val master_msg_name : master_msg -> string

(** Messages from worker to Disco. *)

type output = {
  label : int;
  filename : string;
}

type worker_msg =
  | W_worker of (* version *) string * (* pid *) int
  | W_taskinfo
  | W_input_exclude of input_id list
  | W_input_include of input_id list
  | W_input_failure of input_id * replica_id list
  | W_message of string
  | W_error of string
  | W_fatal of string
  | W_output of output * (* size *) int
  | W_done

val protocol_version : string

(** One message exchange using the request-response protocol. *)
val send_request : worker_msg -> in_channel -> out_channel -> master_msg

(** parse an index payload *)
val parse_index : string -> (int * (string * int)) list
