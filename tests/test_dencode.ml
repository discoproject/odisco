module B = Dencode

let passvals = [ B.Int 1L;
                 B.String "bencode";
                 B.String "";
                 B.List [B.Int 1L; B.String ""; B.String "bencode"];
                 B.Dict ["", B.Int 1L; "bencode", B.String ""];
                 B.Dict ["a", B.Dict ["b", B.Int 2L]; "b", B.List []];
                 B.Dict [];
                 B.List [];
               ]

let check_pass v =
  let s = B.to_string v in
  let v' = B.of_string s in
    if v' <> v then begin
      Printf.printf "Failure: %s <> %s\n" s (B.to_string v');
      exit 1
    end

let pass () =
  try
    List.iter check_pass passvals
  with B.Parse_error e ->
    Printf.printf "%s\n" (B.string_of_error e);
    exit 1

let failvals = [ "cde", B.Unexpected_char (0, 'c', None);
                 "d:e", B.Unexpected_char (1, ':', None);
                 "ie", B.Unexpected_char (1, 'e', Some "int");
                 "123", B.Expected_char (0, ':', "string");
                 "i2323", B.Unterminated_value (0, "int");
                 "i3age", B.Invalid_value (0, "int");
                 "", B.Empty_string 0;
                 "di2e2:abe", B.Invalid_key_type (1, "int");
                 "1ss:wewew", B.Invalid_string_length (0, "1ss");
               ]

let check_fail (s, e) =
  try
    ignore (B.of_string s)
  with B.Parse_error e' ->
    if e <> e' then
      Printf.printf "Failure: Got error %s, expected %s.\n"
        (B.string_of_error e') (B.string_of_error e)

let fail () =
  List.iter check_fail failvals

let _ =
  pass ();
(*  fail ()*)
