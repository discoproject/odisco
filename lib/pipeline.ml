module J  = Json
module JC = Json_conv
module U  = Utils
module P  = Protocol
module E  = Errors

type label = int
type stage = string

type grouping =
  | Split
  | Group_label
  | Group_node
  | Group_node_label
  | Group_all

type pipeline = (stage * grouping) list

type data_size = int

type job_input = label * data_size * Uri.t list

type task_input =
  | Data of label * Uri.t
  | Dir_indexed of label * Uri.t
  | Dir of Uri.t

type pipeline_error =
  | Invalid_grouping of string
  | Invalid_pipeline_json of J.t
  | Invalid_pipeline_stage of string
  | Invalid_pipeline of string

exception Pipeline_error of pipeline_error

type job_input_error =
  | Invalid_job_input_json of J.t
  | Invalid_job_input_string of string
  | Invalid_job_input_label of string
  | Invalid_job_input_size of string
  | Invalid_job_input_url of string
  | Invalid_job_input of string

exception Job_input_error of job_input_error

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

let string_of_job_input_error = function
  | Invalid_job_input_json j ->
      Printf.sprintf "'%s' is not a valid input in json" (J.to_string j)
  | Invalid_job_input_string i ->
      Printf.sprintf "'%s' is not a valid input specification" i
  | Invalid_job_input_label l ->
      Printf.sprintf "'%s' is not a valid input label" l
  | Invalid_job_input_size z ->
      Printf.sprintf "'%s' is not a valid input size" z
  | Invalid_job_input_url u ->
      Printf.sprintf "'%s' is not a valid input url" u
  | Invalid_job_input e ->
      Printf.sprintf "invalid input (%s)" e

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

(* job input utilities *)

let raw_job_input_of_json i =
  let it = JC.to_list i in
  match it with
  | il :: is :: iurls :: [] ->
      let label, size = JC.to_int il, JC.to_int is in
      let urls = List.map (fun u -> Uri.of_string (JC.to_string u)
                          ) (JC.to_list iurls) in
      label, size, urls
  | _ ->
      raise (Job_input_error (Invalid_job_input_json i))

let job_input_of_json i =
  try raw_job_input_of_json i
  with
  | Job_input_error _ as e ->
      raise e
  | Uri.Uri_error e ->
      raise (Job_input_error (Invalid_job_input (Uri.string_of_error e)))
  | JC.Json_conv_error e ->
      raise (Job_input_error (Invalid_job_input (JC.string_of_error e)))

let job_input_of_string i =
  match U.string_split i ',' with
  | l :: z :: us ->
      let label =
        try int_of_string l
        with Failure _ -> raise (Job_input_error (Invalid_job_input_label l)) in
      let size =
        try int_of_string z
        with Failure _ -> raise (Job_input_error (Invalid_job_input_size z)) in
      let urls = List.map
          (fun u ->
            try Uri.of_string u
            with _ -> raise (Job_input_error (Invalid_job_input_url u))
          ) us in
      label, size, urls
  | _ ->
      raise (Job_input_error (Invalid_job_input_string i))

let json_of_job_input (label, size, urls) =
  let inputs = List.map (fun u -> J.String (Uri.to_string u)) urls in
  J.Array (Array.of_list
             [JC.of_int label; JC.of_int size; J.Array (Array.of_list inputs)])

(* task input utilities *)

let uri_of = function
  | Data (_, u)
  | Dir_indexed (_, u)
  | Dir u              -> u

let task_input_of_one_uri ti id (rid, uri) =
  let trans_auth =
    match uri.Uri.scheme, uri.Uri.authority with
    | Some "dir", Some a
    | Some "disco", Some a -> Some {a with Uri.port = Some ti.P.task_disco_port}
    | _, auth              -> auth in
  let url =
    match uri.Uri.scheme, uri.Uri.fragment with
    | Some "dir", None ->
        Dir {uri with Uri.authority = trans_auth}
    | Some "dir", Some l ->
        (try
          let label = int_of_string l
          in Dir_indexed (label, {uri with Uri.authority = trans_auth;
                                  fragment = None})
        with Failure _ ->
          raise (E.Worker_failure (E.Invalid_task_input_label (id, rid, uri))))
    | _, Some l ->
        (try
          let label = int_of_string l
          in Data (label, {uri with Uri.authority = trans_auth;
                           fragment = None})
        with Failure _ ->
          raise (E.Worker_failure (E.Invalid_task_input_label (id, rid, uri))))
    | _, None ->
        raise (E.Worker_failure (E.Missing_task_input_label (id, rid, uri)))
  in rid, url

let comparable_task_inputs (_, i1) (_, i2) =
  match i1, i2 with
  | Data (l1, _), Data (l2, _)
  | Dir_indexed (l1, _), Dir_indexed (l2, _) -> l1 = l2
  | Dir _, Dir _                             -> true
  | _, _                                     -> false

let task_input_of ti id = function
  | [] ->
      []
  | (r :: _) as replicas ->
      let one = task_input_of_one_uri ti id r in
      let all = List.map (task_input_of_one_uri ti id) replicas in
      (* ensure all urls in the input are consistent *)
      if not (List.for_all (comparable_task_inputs one) all)
      then
        let uris = List.map snd replicas in
        raise (E.Worker_failure (E.Inconsistent_task_inputs (id, uris)))
      else all
