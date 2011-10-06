(* This module captures the minimal interface needed from the worker
   environment: i.e. the local filesystem and HTTP. *)

(* file wrapper to distinguish between transient and persistent files *)

module File : sig
  type t = {
    name : string;
    fd : Unix.file_descr;
    delete_on_close : bool;
  }
  val name : t -> string
  val fd : t -> Unix.file_descr
  val close : t -> unit
end

type input_req = Protocol.input_id * (* list of alternative replica locations *) Uri.t list

(* download files at specified locations and return open file handles
   to their local copies
*)
type input_resp = Protocol.input_id * (Errors.error, (Uri.t * File.t)) Utils.lr
val inputs_from : Protocol.taskinfo -> input_req list -> input_resp list

(* retrieve uncompressed payloads from specified locations *)
type payload_resp = Protocol.input_id * (Errors.error, (Uri.t * string)) Utils.lr
val payloads_from : Protocol.taskinfo -> input_req list -> payload_resp list

(* parse an index payload *)
val parse_index : string -> (string * string) list

(* open a task output file *)
val open_output_file : Protocol.taskinfo -> int option -> File.t

(** client-side environment *)

(** default Disco/DDFS master: if the DISCO_MASTER_HOST is not set,
    this is derived from the local hostname *)
val default_master_host : unit -> string

(** default Disco/DDFS port: if the DISCO_PORT is not set, this
    defaults to 8989 *)
val default_port : unit -> int
