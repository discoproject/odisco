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

(* utilities *)

val open_output_file : Protocol.taskinfo -> int option -> File.t
val read_index : Protocol.taskinfo -> Uri.t list -> (string * string) list

type download_result =
  | Download_file of File.t * Uri.t
  | Download_error of Errors.error

val download : Uri.t list -> Protocol.taskinfo -> download_result
