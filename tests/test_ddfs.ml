module U = Utils
module E = Errors
module D = Ddfs

module StringMap = Map.Make (struct type t = string let compare = compare end)

let tAG_LIST_TIMEOUT = 5.0
let tAG_TIMEOUT = 60.0
let bLOB_TIMEOUT = 0.3

let verbose = ref false

let print_usage () =
  Printf.printf "%s: [-v] [cmd ...]\n" Sys.argv.(0);
  Printf.printf "where cmd can be:\n";
  Printf.printf "    -list        : list all tag names\n";
  Printf.printf "    -size  <tag> : print the (unreplicated) size of <tag>\n";
  Printf.printf "    -get   <tag> : print the metadata of <tag>\n";
  Printf.printf "    -links <tag> : print (first-level) child tags of <tag>\n";
  Printf.printf "    -links_all   : print (first-level) child tags\n";
  Printf.printf "    -size_all    : print (unreplicated) size of all tags\n";
  exit 1

let tag_show tag_name =
  match D.tag_of_tagname ~timeout:tAG_TIMEOUT tag_name with
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

let tag_links tag_name =
  match D.child_tags_of_tag_name ~timeout:tAG_TIMEOUT tag_name with
    | U.Left e ->
      Printf.eprintf "%s\n" (E.string_of_error e)
    | U.Right children ->
      Printf.printf "%s: " tag_name;
      List.iter (Printf.printf "%s ") children;
      Printf.printf "\n"

let tag_links_all () =
  match D.tag_list ~timeout:tAG_LIST_TIMEOUT () with
    | U.Left e ->
      Printf.eprintf "%s\n" (E.string_of_error e)
    | U.Right tl ->
      List.iter tag_links tl

let kB = 1000
let mB = 1000000
let gB = 1000000000
let tB = 1000000000000
let human_size size =
  if size < kB then Printf.sprintf "%dB" size
  else if size < mB then Printf.sprintf "%dKB" (size / kB)
  else if size < gB then Printf.sprintf "%dMB" (size / mB)
  else if size < tB then Printf.sprintf "%dGB" (size / gB)
  else Printf.sprintf "%dTB" (size / tB)

let log_tag_size ?(partial = false) tag_name size =
  if partial then
    Printf.printf "tag %s contains %s (and probably more; some blobs were inaccessible)\n%!"
      tag_name (human_size size)
  else
    Printf.printf "tag %s contains %s\n%!" tag_name (human_size size)

let rec tag_size_helper tag_name visited ~had_error ~show_children =
  if StringMap.mem tag_name visited then
    (StringMap.find tag_name visited), had_error, visited
  else begin
    U.dbg "processing tag %s ..." tag_name;
    match D.tag_of_tagname ~timeout:tAG_TIMEOUT tag_name with
      | U.Left e ->
        Printf.eprintf "%s\n" (E.string_of_error e);
        0, true, visited
      | U.Right t ->
        let acc, had_error, visited =
          List.fold_left (fun (acc, had_err, visited) blobset ->
            match blobset with
              | [] -> acc, had_err, visited
              | (b :: _) as blobs ->
                if D.is_tag_url b then begin
                  let tn = D.tag_name_of_url b in
                  let sz, err, v = tag_size_helper tn visited ~had_error:false ~show_children in
                  acc + sz, (err || had_err), v
                end else begin
                  match D.blob_size ~timeout:bLOB_TIMEOUT blobs with
                    | None ->
                      Printf.eprintf "Tag %s: error sizing blobs: %s\n"
                        tag_name (String.concat ", " (List.map Uri.to_string blobs));
                      0, true, visited
                    | Some bsz ->
                      acc + bsz, had_err, visited
                end
          ) (0, false, visited) t.D.tag_urls in
        if show_children then log_tag_size ~partial:had_error tag_name acc;
        acc, had_error, (StringMap.add tag_name acc visited)
  end

let tag_size tag_name =
  let tsz, partial, _ =
    tag_size_helper tag_name StringMap.empty ~had_error:false ~show_children:false
  in log_tag_size ~partial tag_name tsz

let tag_list () =
  match D.tag_list ~timeout:tAG_LIST_TIMEOUT () with
    | U.Left e ->
      Printf.eprintf "%s\n" (E.string_of_error e)
    | U.Right tl ->
      List.iter (Printf.printf "%s\n") tl

let tag_size_all () =
  match D.tag_list ~timeout:tAG_LIST_TIMEOUT () with
    | U.Left e ->
      Printf.eprintf "%s\n" (E.string_of_error e)
    | U.Right tl ->
      let size_map =
        List.fold_left (fun v tn ->
          let _, _, v' = tag_size_helper tn v ~had_error:false ~show_children:true
          in v'
        ) StringMap.empty tl in
      let total = StringMap.fold (fun _t sz acc -> sz + acc) size_map 0 in
      Printf.printf "Total data in DDFS is %s (unreplicated).\n" (human_size total)

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
         | "-links" ->
           let tags, next = get_op_args (indx + 1) in
           List.iter tag_links tags;
           process_args next
         | "-list" ->
           tag_list ();
           process_args (indx + 1)
         | "-size_all" ->
           tag_size_all ();
           process_args (indx + 1)
         | "-links_all" ->
           tag_links_all ();
           process_args (indx + 1)
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
