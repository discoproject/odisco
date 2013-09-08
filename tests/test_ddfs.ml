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
let mAX_ORD = 4
let human_size size =
  let rec iter ord size =
    if size < kB || ord == mAX_ORD then size, ord
    else iter (ord + 1) (size / 1000) in
  let hsz, ord = iter 0 size in
  let unit = match ord with
    | 0 -> "B"
    | 1 -> "KB"
    | 2 -> "MB"
    | 3 -> "GB"
    | 4 (* mAX_ORD *) | _ -> "TB" in
  Printf.sprintf "%d%s" hsz unit

let log_tag_size ?(errs = (0, 0)) tag_name size =
  if errs <> (0, 0) then
    Printf.printf "tag %s contains %s (and probably more; some %d nested tags and %d blobs were inaccessible)\n%!"
      tag_name (human_size size) (fst errs) (snd errs)
  else
    Printf.printf "tag %s contains %s\n%!" tag_name (human_size size)

let rec tag_size_helper tag_name visited ((tag_errs, blob_errs) as errs) ~show_children =
  if StringMap.mem tag_name visited then
    (StringMap.find tag_name visited), errs, visited
  else begin
    U.dbg "processing tag %s ..." tag_name;
    match D.tag_of_tagname ~timeout:tAG_TIMEOUT tag_name with
      | U.Left e ->
        Printf.eprintf "%s\n" (E.string_of_error e);
        0, (tag_errs + 1, blob_errs), visited
      | U.Right t ->
        let acc, ((t_errs, b_errs) as new_errs), visited =
          List.fold_left (fun (acc, ((t_errs, b_errs) as errs), visited) blobset ->
            match blobset with
              | [] -> acc, errs, visited
              | (b :: _) as blobs ->
                if D.is_tag_url b then begin
                  let tn = D.tag_name_of_url b in
                  let sz, (t_e, b_e), v = tag_size_helper tn visited (0, 0) ~show_children in
                  acc + sz, (t_errs + t_e, b_errs + b_e), v
                end else begin
                  match D.blob_size ~timeout:bLOB_TIMEOUT blobs with
                    | None ->
                      Printf.eprintf "Tag %s: error sizing blobs: %s\n"
                        tag_name (String.concat ", " (List.map Uri.to_string blobs));
                      0, (t_errs, b_errs+1), visited
                    | Some bsz ->
                      acc + bsz, errs, visited
                end
          ) (0, (0, 0), visited) t.D.tag_urls in
        if show_children then log_tag_size ~errs:new_errs tag_name acc;
        acc, (tag_errs + t_errs, blob_errs + b_errs), (StringMap.add tag_name acc visited)
  end

let tag_size tag_name =
  let tsz, errs, _ =
    tag_size_helper tag_name StringMap.empty (0, 0) ~show_children:false
  in log_tag_size ~errs tag_name tsz

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
          let _, _, v' = tag_size_helper tn v (0, 0) ~show_children:true
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
  U.init_logfile "/tmp";
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
