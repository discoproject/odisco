(** The Disco worker protocol. *)

(** Messages from Disco to worker. *)

type stage =
  | Map
  | Reduce

val string_of_stage : stage -> string

type taskinfo = {
  (* info from protocol *)
  task_id : int;
  task_stage : stage;
  task_name : string;
  task_host : string;
  task_disco_port : int;
  task_put_port : int;
  task_disco_root : string;
  task_ddfs_root : string;

  (* runtime state *)
  mutable task_rootpath : string;
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

type input_id = int
type replica_id = int
type replica = replica_id * Uri.t
type input = input_id * input_status * replica list

type master_msg =
  | M_ok
  | M_die
  | M_taskinfo of taskinfo
  | M_task_input of task_input_status * input list
  | M_retry of replica list
  | M_fail

val master_msg_name : master_msg -> string

(** Messages from worker to Disco. *)

type output_type =
  | Data
  | Labeled
  | Persistent

type output = {
  label : string option;
  filename : string;
  otype : output_type;
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
  | W_output of output
  | W_done

val protocol_version : string

(** One message exchange using the request-response protocol. *)

val send_request : worker_msg -> in_channel -> out_channel -> master_msg

(** Utilities to process the URIs used in the protocol. *)

val norm_uri : taskinfo -> Uri.t -> Uri.t
val scheme_of_uri : Uri.t -> scheme
