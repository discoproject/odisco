module H = Http
module C = Http_client
module P = Pipeline
module J = Jobpack
module U = Utils
module Api = Rest_api

type op =
  | Output of string
  | Check of string
  | Submit

let name   = ref None
let worker = ref None
let pipe   = ref []
let inputs = ref []
let op     = ref None

let parse_args () =
  let options = Arg.align [("-n", Arg.String (fun n -> name := Some n),
                            " job name (will be prefix of actual job)");
                           ("-w", Arg.String (fun w -> worker := Some w),
                            " path to executable to run");
                           ("-p", Arg.String (fun p -> pipe := P.pipeline_of_string p),
                            " job pipeline (as described above)");
                           ("-o", Arg.String (fun o -> op := Some (Output o)),
                            " file to save jobpack into");
                           ("-c", Arg.String (fun o -> op := Some (Check o)),
                            " jobpack file to check");
                           ("-s", Arg.Unit (fun () -> op := Some Submit),
                            " submit job")] in
  let usage = (Printf.sprintf
                 "Usage: %s -n <jobname> -w <worker> -p <pipeline> <OP> <input> <input> ...\n%s"
                 Sys.argv.(0)
                 (String.concat "\n" [
                   "  The <pipeline> is expressed as a sequence of stages:";
                   "    <stage>,<group>:<stage>,<group>:...";
                   "    where <stage> is a string, and <group> is one of the following:";
                   "        split, join_node, join_label, join_node_label, join_all";
                   "  Each input is specified as:";
                   "    <label>,<size>,<url>,<url>,...";
                   "  The <OP> operation to perform is one of:";
                   "    -o <filename> | -c <filename> | -s"])) in
  Arg.parse options (fun i -> inputs := P.input_of_string i :: !inputs) usage;
  let needs_info = function | Output _ -> true | Check _ -> false | Submit -> true in
  match !op, !name, !worker with
    | None, _, _ ->
      Printf.eprintf "No operation specified.\n";
      Arg.usage options usage; exit 1
    | o, None, _ when needs_info (U.unopt o) ->
      Printf.eprintf "No jobname specified.\n";
      Arg.usage options usage; exit 1
    | o, _, None when needs_info (U.unopt o) ->
      Printf.eprintf "No job executable specified.\n";
      Arg.usage options usage; exit 1
    | Some (Output o), Some n, Some w ->
      `Output (o, n, w)
    | Some Submit, Some n, Some w ->
      `Submit (n, w)
    | Some (Check o), _, _ ->
      `Check o
    | _, _, _ -> assert false

let print_exception e =
  let msg = match e with
    | J.Jobpack_error je ->
      Printf.sprintf "bad jobpack: %s" (J.string_of_error je)
    | P.Pipeline_error pe ->
      Printf.sprintf "bad pipeline: %s" (P.string_of_pipeline_error pe)
    | P.Input_error ie ->
      Printf.sprintf "bad input: %s" (P.string_of_input_error ie)
    | Sys_error s ->
      Printf.sprintf "%s" s
    | Unix.Unix_error (ec, fn, _) ->
      Printf.sprintf "%s: %s" (Unix.error_message ec) fn
    | e ->
      Printf.sprintf "%s" (Printexc.to_string e)
  in Printf.eprintf "%s\n" msg

let get_user () =
  try Unix.getlogin ()
  with _-> (Unix.getpwuid (Unix.getuid ())).Unix.pw_name

let save_jobpack ofile pack =
  let ofd = Unix.openfile ofile [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC] 0o640 in
  ignore (Unix.write ofd pack 0 (String.length pack));
  Unix.close ofd;
  Printf.printf "Jobpack of size %d saved in %s.\n" (String.length pack) ofile

let gen_jobpack name worker =
  let owner = Printf.sprintf "%s@%s" (get_user ()) (Unix.gethostname ()) in
  let pipeline, inputs = !pipe, !inputs in
  ((J.make_jobpack ~name ~worker ~owner ~pipeline inputs) :> string)

let show_pipeline p =
  Printf.printf "Pipeline:";
  List.iter (fun (s, g) ->
    Printf.printf " -> %s (%s)" s (P.string_of_grouping g)
  ) p;
  Printf.printf "\n"

let show_inputs i =
  Printf.printf "Inputs:\n";
  List.iter (fun (l, s, urls) ->
    Printf.printf "\t%d %d [" l s;
    List.iter (fun u -> Printf.printf " %s" (Uri.to_string u)) urls;
    Printf.printf " ]\n"
  ) i

let chk_jobpack file =
  let jobpack = U.contents_of_file file in
  let hdr = J.header_of jobpack in
  let jobdict = J.jobdict_of hdr jobpack in
  let open J in
  Printf.printf "%s contains job '%s' by '%s' running '%s'.\n"
    file jobdict.name jobdict.owner jobdict.worker;
  show_pipeline (jobdict.pipeline :> (string * P.grouping) list);
  show_inputs (jobdict.inputs :> (int * int * Uri.t list) list)

let submit_jobpack ?cfg ?timeout pack =
  let url = Api.url_for_job_submit (Cfg.safe_config cfg) in
  let err_of e = Failure (C.string_of_error e) in
  match (Api.payload_of_req ?timeout (H.Post, C.Payload([url], Some pack), 0)
           err_of) with
    | U.Left e -> print_exception e
    | U.Right r -> Printf.printf "Submitted job: %s\n" r

let _ =
  try match parse_args () with
    | `Output (o, n, w) -> save_jobpack o (gen_jobpack n w)
    | `Check o          -> chk_jobpack o
    | `Submit (n, w)    -> submit_jobpack (gen_jobpack n w)
  with e ->
    print_exception e; exit 1
