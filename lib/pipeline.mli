type label = int
type stage = string

type grouping =
  | Split
  | Join_label
  | Join_node
  | Join_node_label
  | Join_all

type pipeline = (stage * grouping) list

type data_size = int

type input = label * data_size * Uri.t list

type pipeline_error =
  | Invalid_grouping of string
  | Invalid_pipeline_json of Json.t
  | Invalid_pipeline_stage of string
  | Invalid_pipeline of string

type input_error =
  | Invalid_input_json of Json.t
  | Invalid_input_string of string
  | Invalid_input_label of string
  | Invalid_input_size of string
  | Invalid_input_url of string
  | Invalid_input of string

val grouping_of_string : string -> grouping
val string_of_grouping : grouping -> string

val pipeline_of_string : string -> pipeline
val pipeline_of_json : Json.t -> pipeline
val json_of_pipeline : pipeline -> Json.t

val input_of_string : string -> input
val input_of_json : Json.t -> input
val json_of_input : input -> Json.t

exception Pipeline_error of pipeline_error
exception Input_error of input_error

val string_of_pipeline_error : pipeline_error -> string
val string_of_input_error : input_error -> string
