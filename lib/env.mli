(** This module captures the minimal interface needed from the worker
    environment: i.e. the local filesystem and HTTP. *)

(** file wrapper to distinguish between transient and persistent files *)

module File : sig
  type t = {
    name : string;
    fd : Unix.file_descr;
    delete_on_close : bool;
  }
  val name : t -> string
  val fd : t -> Unix.file_descr
  val close : t -> unit
  val size : t -> int
end

type 'a input_req =
    Protocol.input_id * 'a * (* list of replica locations *) Uri.t list

(** download files at specified locations and return open file handles
   to their local copies
 *)
type 'a input_resp =
    Protocol.input_id * 'a * (Errors.error, (Uri.t * File.t)) Utils.lr

val inputs_from :
    Protocol.taskinfo -> ('a input_req) list -> ('a input_resp) list

(** retrieve contents of specified urls without creating local file
    copies of remote data.  this is typically used to retrieve index
    files, which are assumed to be small enough to hold in memory
 *)

type 'a payload_resp =
    Protocol.input_id * 'a * (Errors.error, (Uri.t * string)) Utils.lr

val payloads_from :
    Protocol.taskinfo -> ('a input_req) list -> ('a payload_resp) list

(** open a task output file *)
val open_output_file : Protocol.taskinfo -> int -> File.t

(** client-side environment *)

(** default Disco/DDFS master: if the DISCO_MASTER_HOST is not set,
    this is derived from the local hostname *)
val default_master_host : unit -> string

(** default Disco/DDFS port: if the DISCO_PORT is not set, this
    defaults to 8989 *)
val default_port : unit -> int
