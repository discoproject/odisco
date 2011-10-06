module U = Utils
module E = Errors
module D = Ddfs

let print_usage () =
  Printf.printf "%s: [options]\n" Sys.argv.(0);
  Printf.printf "    -size <tag> : print the (unreplicated) size of the tag\n";
  exit 1

let rec tag_size_helper tag_name had_error =
  Printf.printf "[tag %s]\n" tag_name;
  match D.tag_of_tagname tag_name with
    | U.Left e ->
      Printf.printf "Error retrieving tag %s: %s\n" tag_name (E.string_of_error e);
      0, true
    | U.Right t ->
      let tsz, err =
        List.fold_left
          (fun (acc, had_err) blobset ->
            match blobset with
              | [] -> acc, had_err
              | b :: _ ->
                if D.is_tag_url b then begin
                  let tn = D.tag_name b in
                  let sz, err = tag_size_helper tn had_err in
                  acc + sz, err
              end else begin
                match D.blob_size blobset with
                  | None ->
                    Printf.eprintf "Tag %s: error sizing blobs: %s\n"
                      tag_name (String.concat ", " (List.map Uri.to_string blobset));
                    0, true
                  | Some bsz ->
                    Printf.printf "[+%d : %s]\n"
                      bsz (String.concat ", " (List.map Uri.to_string blobset));
                    acc + bsz, had_err
              end
          ) (0, had_error) t.D.tag_urls in
      Printf.printf "[%d -> %s]\n" tsz tag_name;
      tsz, err

let tag_size tag_name =
  let tsz, err = tag_size_helper tag_name false in
  if err then
    Printf.printf "Error computing tag size for %s: partial estimate is %d bytes.\n"
      tag_name tsz
  else
    Printf.printf "Tag %s has %d bytes.\n" tag_name tsz

let run () =
  let is_opt a = a.[0] = '-' in
  let num_args = Array.length Sys.argv in
  let get_op_args indx =
    let rec helper i acc =
      if i >= num_args || is_opt Sys.argv.(i)
      then List.rev acc, i
      else helper (i+1) (Sys.argv.(i) :: acc)
    in helper indx [] in
  let rec process_args indx =
    if indx >= num_args then ()
    else
      (let opt = Sys.argv.(indx) in
       match opt with
         | "-size" ->
           let tags, next = get_op_args (indx + 1) in
           List.iter tag_size tags;
           process_args next
         | _ ->
           Printf.printf "Unrecognized option: %s\n" opt;
           print_usage ()
      )
  in process_args 1

let _ =
  if Array.length Sys.argv = 1 then
    print_usage ();
  Printexc.record_backtrace true;
  try run ()
  with
    | e ->
      Printf.eprintf "%s\n%s\n%!"
        (Printexc.to_string e)
        (Printexc.get_backtrace ())
