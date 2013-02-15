module P = Protocol
module L = Pipeline
module E = Errors

let task_input_of_one_uri ti id label (rid, uri) =
  let trans_auth =
    match uri.Uri.scheme, uri.Uri.authority with
    | Some "dir", Some a
    | Some "disco", Some a -> Some {a with Uri.port = Some ti.P.task_disco_port}
    | _, auth              -> auth in
  let url =
    match uri.Uri.scheme, label with
    | Some "dir", P.Input_label_all ->
        L.Dir {uri with Uri.authority = trans_auth}
    | Some "dir", P.Input_label l ->
        L.Dir_indexed (l, {uri with Uri.authority = trans_auth;
                           fragment = None})
    | _, P.Input_label l ->
        L.Data (l, {uri with Uri.authority = trans_auth;
                    fragment = None})
    | _, P.Input_label_all ->
        raise (E.Worker_failure (E.Invalid_task_input_label (id, rid, uri)))
  in rid, url

let comparable_task_inputs (_, i1) (_, i2) =
  match i1, i2 with
  | L.Data (l1, _), L.Data (l2, _)
  | L.Dir_indexed (l1, _), L.Dir_indexed (l2, _) -> l1 = l2
  | L.Dir _, L.Dir _                             -> true
  | _, _                                         -> false

let task_input_of ti id label = function
  | [] ->
      []
  | (r :: _) as replicas ->
      let one = task_input_of_one_uri ti id label r in
      let all = List.map (task_input_of_one_uri ti id label) replicas in
      (* ensure all urls in the input are consistent *)
      if not (List.for_all (comparable_task_inputs one) all)
      then
        let uris = List.map snd replicas in
        raise (E.Worker_failure (E.Inconsistent_task_inputs (id, uris)))
      else all
