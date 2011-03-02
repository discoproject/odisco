module U = Uri

(* handling options *)

let unopt = function
  | Some v -> v
  | None -> assert false

let mapopt f = function
  | Some v -> Some (f v)
  | None -> None

(* split strings at a seperator character *)

let string_split s c =
  let slen = String.length s in
  let rec iter cursor acc =
    if cursor >= slen
    then List.rev (List.filter (fun s -> String.length s > 0) acc)
    else (try
            let pivot = String.index_from s cursor c in
              iter (pivot + 1) (String.sub s cursor (pivot - cursor) :: acc)
          with Not_found ->
            iter slen (String.sub s cursor (slen - cursor) :: acc))
  in iter 0 []

(* whitespace handling and stripping *)

let is_whitespace = function
  | ' ' | '\t' | '\n' | '\r' -> true
  | _ -> false

let strip_word = function
  | "" -> ""
  | w ->
      let len = String.length w in
      let rec starter ofs =
        if ofs = len then len - 1
        else if is_whitespace w.[ofs] then starter (ofs + 1)
        else ofs in
      let rec ender ofs =
        if ofs = 0 then 0
        else if is_whitespace w.[ofs] then ender (ofs - 1)
        else ofs in
      let s = starter 0 in
      let e = ender (len - 1) in
        match s,e with
          | _ when s >= e -> ""
          | 0, _ when e = len - 1 -> w
          | _ -> String.sub w s (e - s + 1)

(* debug logging *)

let verbose = ref true
let logger =
  if !verbose then begin
    let logc = open_out_gen [ Open_creat;
                              Open_append;
                              Open_wronly ] 0o660 "/tmp/oc.dbg" in
      fun s -> Printf.fprintf logc "%s\n%!" s
  end else fun _s -> ()
let dbg fmt =
  Printf.ksprintf logger fmt
