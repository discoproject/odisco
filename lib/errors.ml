module J = Json
module JP = Json_parse
module JC = Json_conv
module C = Http_client

type error =
  | Protocol_parse_error of string * string
  | Protocol_error of string
  | Bad_msg of string * string * string
  | Unknown_msg of string * J.t
  | Unexpected_msg of string
  | Unknown_stage of string
  | Invalid_input of string
  | Input_failure of (C.url * C.error) list
  | Input_local_failure of C.url * Unix.error
  | Input_remote_failure of C.url * int
  | Unsupported_input_scheme of int * string
  | Invalid_port of string
  | Tag_retrieval_failure of string * C.error
  | Tag_list_failure of C.error
  | Invalid_json of JP.error
  | Unexpected_json of JC.error
  | Invalid_uri of Uri.error
  | Invalid_task_input_label of int * int * Uri.t
  | Missing_task_input_label of int * int * Uri.t
  | Invalid_task_input_url of int * int * Uri.t
  | Inconsistent_task_inputs of int * Uri.t list
  | Invalid_dir_index of int * int * Uri.t

let string_of_error = function
  | Protocol_parse_error (i, es) ->
      Printf.sprintf "protocol parse error on input '%s': %s" i es
  | Protocol_error msg ->
      Printf.sprintf "protocol error: %s" msg
  | Bad_msg (m, s, e) ->
      Printf.sprintf "bad '%s' msg: %s (%s)" m s e
  | Unknown_msg (s, v) ->
      Printf.sprintf "unknown msg '%s' (\"%s\")" s (J.string_of_type v)
  | Unexpected_msg m ->
      Printf.sprintf "unexpected msg '%s'" m
  | Unknown_stage s ->
      Printf.sprintf "unknown stage '%s' received" s
  | Invalid_input s ->
      Printf.sprintf "invalid input '%s'" s
  | Input_failure el ->
      let b = Buffer.create 256 in
      Buffer.add_string b "failure retrieving input:";
      List.iter
        (fun (u, e) ->
          Buffer.add_string b (Printf.sprintf " [%s: %s]" u (C.string_of_error e))
        ) el;
    Buffer.contents b
  | Input_local_failure (u, e) ->
      Printf.sprintf "error accessing input '%s': %s" u (Unix.error_message e)
  | Input_remote_failure (u, sc) ->
      Printf.sprintf "error response retrieving input '%s': status %d" u sc
  | Unsupported_input_scheme (id, s) ->
      Printf.sprintf "unsupported input scheme (%d): %s" id s
  | Invalid_port s ->
      Printf.sprintf "Invalid port '%s'" s
  | Tag_retrieval_failure (t, e) ->
      Printf.sprintf "Error retrieving tag '%s': %s" t (C.string_of_error e)
  | Tag_list_failure e ->
      Printf.sprintf "Error listing tags: %s" (C.string_of_error e)
  | Invalid_json e ->
      Printf.sprintf "Invalid JSON: %s" (JP.string_of_error e)
  | Unexpected_json e ->
      Printf.sprintf "Unexpected JSON: %s" (JC.string_of_error e)
  | Invalid_uri e ->
      Printf.sprintf "Invalid uri: %s" (Uri.string_of_error e)
  | Invalid_task_input_label (id, rid, u) ->
      Printf.sprintf "Input %d:%d '%s' does not have a valid input label"
        id rid (Uri.to_string u)
  | Missing_task_input_label (id, rid, u) ->
      Printf.sprintf "Input %d:%d '%s' is missing an expected input label"
        id rid (Uri.to_string u)
  | Invalid_task_input_url (id, rid, u) ->
      Printf.sprintf "Input %d:%d '%s' is invalid"
        id rid (Uri.to_string u)
  | Inconsistent_task_inputs (id, ul) ->
      Printf.sprintf "Input %d has inconsistent urls: %s"
        id (String.concat " " (List.map Uri.to_string ul))
  | Invalid_dir_index (id, rid, uri) ->
      Printf.sprintf "Input %d:%d has an invalid index in %s"
        id rid (Uri.to_string uri)

exception Worker_failure of error
