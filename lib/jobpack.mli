(** job inputs

    These are the data input specifications that appear in jobpacks.
    Each input is specified by a set of urls for replicas, a label,
    and a size.  The size can be 0 if the actual data size is unknown.
*)

type data_size = int
type job_input = Pipeline.label * data_size * Uri.t list

val job_input_of_string : string -> job_input
val job_input_of_json : Json.t -> job_input
val json_of_job_input : job_input -> Json.t

type job_input_error =
  | Invalid_job_input_json of Json.t
  | Invalid_job_input_string of string
  | Invalid_job_input_label of string
  | Invalid_job_input_size of string
  | Invalid_job_input_url of string
  | Invalid_job_input of string

exception Job_input_error of job_input_error

val string_of_job_input_error : job_input_error -> string

(** job packets

    These are job pipeline specifications and zipped archives
    containing code and optionally data that are sent to the master
    for execution.
*)

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
  inputs : job_input list;
  save_results : bool;
}

type jobpack = private string

type jobenvs = (string * string) list

(* Extraction *)

val header_of : string -> header

val jobdict_of : header -> string -> jobdict

val jobenvs_of : header -> string -> jobenvs

val jobdata_of : header -> string -> string

(* Construction *)

val make_jobpack : ?envs:jobenvs -> ?jobdata:string -> ?save:bool
  -> name:string -> owner:string -> worker:string
  -> pipeline:Pipeline.pipeline -> job_input list
  -> jobpack

(* Errors *)

val string_of_error : error -> string
