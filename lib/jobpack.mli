type error =
  | Invalid_magic of int
  | Unsupported_version of int
  | Invalid_header of string
  | Invalid_jobdict_ofs of int
  | Invalid_jobenvs_ofs of int
  | Invalid_jobhome_ofs of int
  | Invalid_jobdata_ofs of int
  | Invalid_jobdict of string
  | Invalid_jobenvs of string
  | Invalid_pipeline of string
  | Missing_jobdict_key of string

exception Jobpack_error of error

type header

type jobdict = {
  name : string;
  owner : string;
  worker : string;
  pipeline : Pipeline.pipeline;
  inputs : Pipeline.job_input list;
}

type jobpack = private string

type jobenvs = (string * string) list

(* Extraction *)

val header_of : string -> header

val jobdict_of : header -> string -> jobdict

val jobenvs_of : header -> string -> jobenvs

val jobdata_of : header -> string -> string

(* Construction *)

val make_jobpack : ?envs:jobenvs -> ?jobdata:string
  -> name:string -> owner:string -> worker:string
  -> pipeline:Pipeline.pipeline -> Pipeline.job_input list
  -> jobpack

(* Errors *)

val string_of_error : error -> string
