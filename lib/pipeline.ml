module J  = Json
module JC = Json_conv
module U  = Utils

type label = int
type stage = string

type grouping =
  | Split
  | Group_label
  | Group_node
  | Group_node_label
  | Group_all

type pipeline = (stage * grouping) list

type pipeline_error =
  | Invalid_grouping of string
  | Invalid_pipeline_json of J.t
  | Invalid_pipeline_stage of string
  | Invalid_pipeline of string

exception Pipeline_error of pipeline_error

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

(* pipeline utilities *)

let grouping_of_string = function
  | "split"            -> Split
  | "group_label"      -> Group_label
  | "group_node"       -> Group_node
  | "group_node_label" -> Group_node_label
  | "group_all"        -> Group_all
  | g                  -> raise (Pipeline_error (Invalid_grouping g))

let string_of_grouping = function
  | Split            -> "split"
  | Group_label      -> "group_label"
  | Group_node       -> "group_node"
  | Group_node_label -> "group_node_label"
  | Group_all        -> "group_all"

let is_valid_pipeline pipe =
  let module StringSet =
    Set.Make (struct type t = stage let compare = compare end) in
  let stages = (List.fold_left (fun stages (s, _g) -> StringSet.add s stages)
                  StringSet.empty pipe)
  in StringSet.cardinal stages = List.length pipe

let json_of_pipeline p =
  let l = List.map (fun (s, g) -> [J.String s; J.String (string_of_grouping g)]
                   ) p
  in J.Array (Array.of_list (List.map (fun sg -> J.Array (Array.of_list sg)) l))

let pipeline_of_json p =
  try
    let sgl = List.map JC.to_list (JC.to_list p) in
    let pipeline =
      List.map
        (function
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

(* task input utilities *)

type task_input =
  | Data of label * Uri.t
  | Dir_indexed of label * Uri.t
  | Dir of Uri.t

let uri_of = function
  | Data (_, u)
  | Dir_indexed (_, u)
  | Dir u              -> u
