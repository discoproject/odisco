type t =
  | Int of int64
  | String of string
  | List of t list
  | Dict of (string * t) list

val string_of_type: t -> string

val is_int: t -> bool
val is_string: t -> bool
val is_dict: t -> bool
val is_list: t -> bool

val is_scalar: t -> bool

val int_val: t -> int64
val string_val: t -> string
val list_val: t -> t list
val dict_val: t -> (string * t) list

(* first integer argument is the offset into the parse string *)
type error =
  | Unexpected_char of int * char * (* bencode type *) string option
  | Expected_char of int * char * (* bencode type *) string
  | Unterminated_value of int * string
  | Invalid_value of int * string
  | Empty_string of int
  | Invalid_key_type of int * (* bencode type *) string
  | Invalid_string_length of int * string

exception Parse_error of error
val string_of_error: error -> string

(* parser and marshaller *)
val of_string: string -> t
val to_string: t -> string
