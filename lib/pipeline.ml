module J = Json
module JC = Json_conv

type label = int
type stage = string

type grouping =
  | Split
  | Join_label
  | Join_node
  | Join_node_label
  | Join_all

type pipeline = (stage * grouping) list

type data_size  = int
type input = label * data_size * Uri.t list

type pipeline_error =
  | Invalid_grouping of string
  | Invalid_pipeline_json of J.t

type input_error =
  | Invalid_input_json of J.t
  | Invalid_input_url of string
  | Invalid_input of string

exception Pipeline_error of pipeline_error
exception Input_error of input_error

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
    List.map (function
                | (s :: g :: []) ->
                  JC.to_string s, (grouping_of_string (JC.to_string g))
                | _ ->
                  raise (Pipeline_error (Invalid_pipeline_json p))
             ) sgl
  with
    | Pipeline_error _ as e ->
      raise e
    | _ ->
      raise (Pipeline_error (Invalid_pipeline_json p))

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

let json_of_input (label, size, urls) =
  let inputs = List.map (fun u -> J.String (Uri.to_string u)) urls in
  J.Array (Array.of_list
             [JC.of_int label; JC.of_int size; J.Array (Array.of_list inputs)])
