module U = Utils
module E = Errors
module D = Ddfs

module StringSet = Set.Make (struct type t = string let compare = compare end)

let verbose = ref false

let print_usage () =
  Printf.printf "%s: [-v] [cmd ...]\n" Sys.argv.(0);
  Printf.printf "where cmd can be:\n";
  Printf.printf "    -size <tag> : print the (unreplicated) size of the tag\n";
  Printf.printf "    -get  <tag> : print the tag metadata\n";
  Printf.printf "    -nest <tag> : print (first-level) nested tags\n";
  exit 1

let tag_show tag_name =
  match D.tag_of_tagname tag_name with
    | U.Left e ->
      Printf.eprintf "%s\n" (E.string_of_error e)
    | U.Right t ->
      Printf.printf "id: %s\n" t.D.tag_id;
      Printf.printf "last_modified: %s\n" t.D.tag_last_modified;
      Printf.printf "attribs:\n";
      List.iter (fun (k, v) -> Printf.printf "\t%s=%s\n" k v) t.D.tag_attribs;
      Printf.printf "urls:\n";
      List.iter (fun bs ->
        Printf.printf "\t%s\n" (String.concat ", " (List.map Uri.to_string bs))
      ) t.D.tag_urls

let tag_nest tag_name =
  match D.tag_of_tagname tag_name with
    | U.Left e ->
      Printf.eprintf "%s\n" (E.string_of_error e)
    | U.Right t ->
      let nested =
        List.fold_left (fun s bs ->
          List.fold_left (fun s u ->
            if D.is_tag_url u then StringSet.add (D.tag_name u) s else s
          ) s bs
        ) StringSet.empty t.D.tag_urls in
      Printf.printf "%s: " tag_name;
      StringSet.iter (Printf.printf "%s, ") nested;
      Printf.printf "\n"

let rec tag_size_helper tag_name visited had_error =
  if StringSet.mem tag_name visited then
    0, had_error, visited
  else begin
    let visited = StringSet.add tag_name visited in
    match D.tag_of_tagname tag_name with
      | U.Left e ->
        Printf.eprintf "%s\n" (E.string_of_error e);
        0, true, visited
      | U.Right t ->
        List.fold_left
          (fun (acc, had_err, visited) blobset ->
            match blobset with
              | [] -> acc, had_err, visited
              | (b :: _) as blobs ->
                if D.is_tag_url b then begin
                  let tn = D.tag_name b in
                  let sz, err, v = tag_size_helper tn visited had_err in
                  acc + sz, err, v
                end else begin
                  match D.blob_size blobs with
                    | None ->
                      Printf.eprintf "Tag %s: error sizing blobs: %s\n"
                        tag_name (String.concat ", " (List.map Uri.to_string blobs));
                      0, true, visited
                    | Some bsz ->
                      acc + bsz, had_err, visited
                end
          ) (0, had_error, visited) t.D.tag_urls
  end

let tag_size tag_name =
  let tsz, err, _ = tag_size_helper tag_name StringSet.empty false in
  if err then
    Printf.printf "Error computing tag size for %s: partial estimate is %d bytes.\n"
      tag_name tsz
  else
    Printf.printf "tag %s contains %d bytes\n" tag_name tsz

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
         | "-v" ->
           verbose := true;
           process_args (indx + 1)
         | "-get" ->
           let tags, next = get_op_args (indx + 1) in
           List.iter tag_show tags;
           process_args next
         | "-size" ->
           let tags, next = get_op_args (indx + 1) in
           List.iter tag_size tags;
           process_args next
         | "-nest" ->
           let tags, next = get_op_args (indx + 1) in
           List.iter tag_nest tags;
           process_args next
         | _ ->
           Printf.printf "Unrecognized option: %s\n" opt;
           print_usage ()
      )
  in
  U.init_logger "/tmp";
  process_args 1

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
