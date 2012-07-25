module J  = Json
module JC = Json_conv
module U  = Utils

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
  | Invalid_pipeline_json of J.t
  | Invalid_pipeline_stage of string
  | Invalid_pipeline of string

type input_error =
  | Invalid_input_json of J.t
  | Invalid_input_string of string
  | Invalid_input_label of string
  | Invalid_input_size of string
  | Invalid_input_url of string
  | Invalid_input of string

exception Pipeline_error of pipeline_error
exception Input_error of input_error

(* error printing utilities *)

let string_of_pipeline_error = function
  | Invalid_grouping g ->
    Printf.sprintf "'%s' is not a valid grouping" g
  | Invalid_pipeline_json j ->
    Printf.sprintf "'%s' is not a valid pipeline in json" (J.to_string j)
  | Invalid_pipeline_stage s ->
    Printf.sprintf "'%s' is not a valid pipeline stage" s
  | Invalid_pipeline e ->
    Printf.sprintf "invalid pipeline (%s)" e

let string_of_input_error = function
  | Invalid_input_json j ->
    Printf.sprintf "'%s' is not a valid input in json" (J.to_string j)
  | Invalid_input_string i ->
    Printf.sprintf "'%s' is not a valid input specification" i
  | Invalid_input_label l ->
    Printf.sprintf "'%s' is not a valid input label" l
  | Invalid_input_size z ->
    Printf.sprintf "'%s' is not a valid input size" z
  | Invalid_input_url u ->
    Printf.sprintf "'%s' is not a valid input url" u
  | Invalid_input e ->
    Printf.sprintf "invalid input (%s)" e

(* pipeline utilities *)

let grouping_of_string = function
  | "split"           -> Split
  | "join_label"      -> Join_label
  | "join_node"       -> Join_node
  | "join_node_label" -> Join_node_label
  | "join_all"        -> Join_all
  | g                 -> raise (Pipeline_error (Invalid_grouping g))

let string_of_grouping = function
  | Split           -> "split"
  | Join_label      -> "join_label"
  | Join_node       -> "join_node"
  | Join_node_label -> "join_node_label"
  | Join_all        -> "join_all"

let is_valid_pipeline pipe =
  let module StringSet =
        Set.Make (struct type t = stage let compare = compare end) in
  let stages = (List.fold_left (fun stages (s, _g) -> StringSet.add s stages)
                  StringSet.empty pipe)
  in StringSet.cardinal stages = List.length pipe

let json_of_pipeline p =
  let l = List.map (fun (s, g) -> [J.String s; J.String (string_of_grouping g)]) p
  in J.Array (Array.of_list (List.map (fun sg -> J.Array (Array.of_list sg)) l))

let pipeline_of_json p =
  try
    let sgl = List.map JC.to_list (JC.to_list p) in
    let pipeline =
      List.map (function
                  | (s :: g :: []) ->
                    JC.to_string s, (grouping_of_string (JC.to_string g))
                  | _ ->
                    raise (Pipeline_error (Invalid_pipeline_json p))
      ) sgl in
    if not (is_valid_pipeline pipeline)
    then raise (Pipeline_error (Invalid_pipeline "repeated stages"));
    pipeline
  with
    | Pipeline_error _ as e -> raise e
    | _ -> raise (Pipeline_error (Invalid_pipeline_json p))

let pipeline_of_string p =
  let parse_stage s =
    match U.string_split s ',' with
      | stage :: group :: [] ->  stage, grouping_of_string group
      | _ -> raise (Pipeline_error (Invalid_pipeline_stage s)) in
  List.map parse_stage (U.string_split p ':')

(* input utilities *)

let raw_input_of_json i =
  let it = JC.to_list i in
  match it with
    | il :: is :: iurls :: [] ->
      let label, size = JC.to_int il, JC.to_int is in
      let urls = List.map (fun u -> Uri.of_string (JC.to_string u)) (JC.to_list iurls) in
      label, size, urls
    | _ ->
      raise (Input_error (Invalid_input_json i))

let input_of_json i =
  try raw_input_of_json i
  with
    | Input_error _ as e ->
      raise e
    | Uri.Uri_error e ->
      raise (Input_error (Invalid_input (Uri.string_of_error e)))
    | JC.Json_conv_error e ->
      raise (Input_error (Invalid_input (JC.string_of_error e)))

let input_of_string i =
  match U.string_split i ',' with
    | l :: z :: us ->
      let label = try int_of_string l
                  with Failure _ -> raise (Input_error (Invalid_input_label l)) in
      let size  = try int_of_string z
                  with Failure _ -> raise (Input_error (Invalid_input_size z)) in
      let urls =
        List.map (fun u -> try Uri.of_string u
                           with _ -> raise (Input_error (Invalid_input_url u))
                 ) us in
      label, size, urls
    | _ ->
      raise (Input_error (Invalid_input_string i))

let json_of_input (label, size, urls) =
  let inputs = List.map (fun u -> J.String (Uri.to_string u)) urls in
  J.Array (Array.of_list
             [JC.of_int label; JC.of_int size; J.Array (Array.of_list inputs)])