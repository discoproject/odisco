type t =
  | Int of int64
  | String of string
  | List of t list
  | Dict of (string * t) list

let string_of_type = function
  | Int _    -> "int"
  | String _ -> "string"
  | List _   -> "list"
  | Dict _   -> "dict"

let is_int    = function Int _    -> true | _ -> false
let is_string = function String _ -> true | _ -> false
let is_list   = function List _   -> true | _ -> false
let is_dict   = function Dict _   -> true | _ -> false

let is_scalar v = is_int v || is_string v

let int_val =
  function Int v    -> v | _ -> raise (Invalid_argument "bad argument type")
let string_val =
  function String v -> v | _ -> raise (Invalid_argument "bad argument type")
let list_val =
  function List v   -> v | _ -> raise (Invalid_argument "bad argument type")
let dict_val =
  function Dict v   -> v | _ -> raise (Invalid_argument "bad argument type")

let marshal_string s =
  (string_of_int (String.length s)) ^ ":" ^ s

let rec marshal f = function
  | Int i ->
      f ("i" ^ Int64.to_string i ^ "e")
  | String s ->
      f (marshal_string s)
  | List l ->
      f "l";
      List.iter (marshal f) l;
      f "e"
  | Dict d ->
      f "d";
      List.iter (fun (k,v) ->
                   f (marshal_string k);
                   marshal f v
                ) d;
      f "e"

let to_string t =
  let buf = Buffer.create 2048 in
    marshal (fun s -> Buffer.add_string buf s) t;
    Buffer.contents buf

type error =
  | Unexpected_char of int * char * (* bencode type *) string option
  | Expected_char of int * char * (* bencode type *) string
  | Unterminated_value of int * string
  | Invalid_value of int * string
  | Empty_string of int
  | Invalid_key_type of int * (* bencode type *) string
  | Invalid_string_length of int * string

exception Parse_error of error

let string_of_error = function
  | Unexpected_char (i, c, None) ->
      Printf.sprintf "Unexpected char %c at offset %d" c i
  | Unexpected_char (i, c, Some s) ->
      Printf.sprintf "Unexpected char %c at offset %d in %s" c i s
  | Expected_char (i, c, s) ->
      Printf.sprintf "Expected char %c not present at offset %d in %s" c i s
  | Unterminated_value (i, s) ->
      Printf.sprintf "Unterminated %s value at offset %d" s i
  | Invalid_value (i, s) ->
      Printf.sprintf "Invalid %s value at offset %d" s i
  | Empty_string i ->
      Printf.sprintf "Unexpected end of input at offset %d" i
  | Invalid_key_type (i, s) ->
      Printf.sprintf "Invalid non-string (%s) key at offset %d" s i
  | Invalid_string_length (i, s) ->
      Printf.sprintf "Invalid length '%s' at offset %d" s i

let get_int_substring s start skip =
  let start_ofs = start + skip in
    match (try Some (String.index_from s start_ofs 'e') with _ -> None) with
      | None ->
          raise (Parse_error (Unterminated_value (start, "int")))
      | Some ofs ->
          if ofs <= start_ofs then
            raise (Parse_error (Unexpected_char (start_ofs, 'e', Some "int")))
          else
            String.sub s start_ofs (ofs - start_ofs)

let get_string_substring s start len =
  let slen, ofs =
    match (try Some (String.index_from s start ':') with _ -> None) with
      | None ->
          raise (Parse_error (Expected_char (start, ':', "string")))
      | Some ofs ->
          if ofs = start then
            raise (Parse_error (Unexpected_char (start, 'e', Some "int")))
          else
            let slen = String.sub s start (ofs - start) in
              (try
                 int_of_string slen
               with _ ->
                 raise (Parse_error ((Invalid_string_length (start, slen))))),
          ofs + 1 in
  let end_ofs = ofs + slen in
    if end_ofs > start + len
    then raise (Parse_error (Unterminated_value (start, "string")))
    else String.sub s ofs slen, end_ofs - start

let rec lfolder s start len acc =
  match s.[start] with
    | 'e' ->
        List.rev acc, start + 1
    | _ ->
        let e, consumed = parse_substring s start len in
          lfolder s (start + consumed) (len - consumed) (e :: acc)
and list_fold s start len =
  let list, next_start = lfolder s start len [] in
    list, next_start - start

and dfolder s start len acc =
  match s.[start] with
    | 'e' ->
        List.rev acc, start + 1
    | _ ->
        let k, kc = parse_substring s start len in
        let v, vc = parse_substring s (start + kc) (len - kc) in
        let c = kc + vc in
          if is_string k
          then dfolder s (start + c) (len - c) ((string_val k, v) :: acc)
          else raise (Parse_error (Invalid_key_type (start, string_of_type k)))
and dict_fold s start len =
  let dict, next_start = dfolder s start len [] in
    dict, next_start - start

and parse_substring s start len =
  if len == 0 then raise (Parse_error (Empty_string start))
  else match s.[start] with
    | 'i' ->
        let v = get_int_substring s start 1 in
          (try Int (Int64.of_string v), 2 + (String.length v)
           with _ -> raise (Parse_error (Invalid_value (start, "int"))))
    | '0' .. '9' ->
        let v, consumed = get_string_substring s start len in
          String v, consumed
    | 'l' ->
        let l, consumed = list_fold s (start + 1) (len - 1) in
          List l, consumed + 1
    | 'd' ->
        let d, consumed = dict_fold s (start + 1) (len - 1) in
          Dict d, consumed + 1
    | c ->
        raise (Parse_error (Unexpected_char (start, c, None)))

let of_string s = fst (parse_substring s 0 (String.length s))
