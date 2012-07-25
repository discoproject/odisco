module J  = Json
module JP = Json_parse
module JC = Json_conv
module P  = Pipeline
module Z  = Zip
module F  = Filename
module U  = Utils

type error =
  | Invalid_magic of int
  | Unsupported_version of int
  | Invalid_header of string
  | Invalid_jobdict_ofs of int
  | Invalid_jobenvs_ofs of int
  | Invalid_jobhome_ofs of int
  | Invalid_jobdata_ofs of int
  | Invalid_jobdict of string
  | Invalid_jobenvs of string
  | Invalid_pipeline of string
  | Missing_jobdict_key of string

exception Jobpack_error of error

type header = {
  magic : int;
  version : int;
  jobdict_ofs : int;
  jobenvs_ofs : int;
  jobhome_ofs : int;
  jobdata_ofs : int
}

type jobdict = {
  name : string;
  owner : string;
  worker : string;
  pipeline : P.pipeline;
  inputs : P.input list;
}

type jobpack = string

type jobenvs = (string * string) list

let mAGIC = 0xd5c0
let vERSION = 0x0001
let hDR_LEN = 128
let hDR_LEN_STR = "128"

(* error printing utilities *)

let string_of_error = function
  | Invalid_magic i ->
    Printf.sprintf "invalid magic %x (expected %x)" i mAGIC
  | Unsupported_version v ->
    Printf.sprintf "unsupported version %x (expected %x)" v vERSION
  | Invalid_header e ->
    Printf.sprintf "invalid header (%s)" e
  | Invalid_jobdict_ofs o ->
    Printf.sprintf "invalid jobdict offset (%d)" o
  | Invalid_jobenvs_ofs o ->
    Printf.sprintf "invalid jobenvs offset (%d)" o
  | Invalid_jobhome_ofs o ->
    Printf.sprintf "invalid jobhome offset (%d)" o
  | Invalid_jobdata_ofs o ->
    Printf.sprintf "invalid jobdata offset (%d)" o
  | Invalid_jobdict s ->
    Printf.sprintf "invalid jobdict (%s)" s
  | Invalid_jobenvs s ->
    Printf.sprintf "invalid jobenvs (%s)" s
  | Invalid_pipeline s ->
    Printf.sprintf "invalid pipeline (%s)" s
  | Missing_jobdict_key k ->
    Printf.sprintf "missing jobdict key %s" k

(* bytestring utilities *)

let int16_at s ofs =
  ((int_of_char s.[ofs]) lsl 8) + int_of_char s.[ofs + 1]

let int32_at s ofs =
  (int_of_char s.[ofs] lsl 24) + (int_of_char s.[ofs + 1] lsl 16)
  + (int_of_char s.[ofs + 2] lsl 8) + int_of_char s.[ofs + 3]

let put_int16 i s ofs =
  s.[ofs]     <- char_of_int ((i lsr 8) land 255);
  s.[ofs + 1] <- char_of_int (i land 255)

let put_int32 i s ofs =
  s.[ofs]     <- char_of_int ((i lsr 24) land 255);
  s.[ofs + 1] <- char_of_int ((i lsr 16) land 255);
  s.[ofs + 2] <- char_of_int ((i lsr  8) land 255);
  s.[ofs + 3] <- char_of_int (i land 255)

(* header utilities *)

let extract_header pack =
  if String.length pack < hDR_LEN then
    raise (Jobpack_error
             (Invalid_header ("must be at least " ^ hDR_LEN_STR ^ " bytes")));
  { magic = int16_at pack 0;
    version = int16_at pack 2;
    jobdict_ofs = int32_at pack 4;
    jobenvs_ofs = int32_at pack 8;
    jobhome_ofs = int32_at pack 12;
    jobdata_ofs = int32_at pack 16 }

let validate_header hdr pack =
  if hdr.magic <> mAGIC then
    raise (Jobpack_error (Invalid_magic hdr.magic));
  if hdr.version <> vERSION then
    raise (Jobpack_error (Unsupported_version hdr.version));
  if hdr.jobdict_ofs <> hDR_LEN then
    raise (Jobpack_error (Invalid_jobdict_ofs hdr.jobdict_ofs));
  if hdr.jobenvs_ofs <= hdr.jobdict_ofs then
    raise (Jobpack_error (Invalid_jobenvs_ofs hdr.jobenvs_ofs));
  if hdr.jobhome_ofs < hdr.jobenvs_ofs then
    raise (Jobpack_error (Invalid_jobhome_ofs hdr.jobhome_ofs));
  if hdr.jobdata_ofs < hdr.jobhome_ofs
    || hdr.jobdata_ofs > String.length pack then
    raise (Jobpack_error (Invalid_jobdata_ofs hdr.jobdata_ofs))

let header_of pack =
  let hdr = extract_header pack in
  validate_header hdr pack;
  hdr

(* jobdict utilities *)

let json_jobdict_of hdr pack =
  let jobdict_len = hdr.jobenvs_ofs - hdr.jobdict_ofs in
  try
    let json = JP.of_substring pack hdr.jobdict_ofs jobdict_len in
    JC.object_table_to_list (JC.to_object_table json)
  with
    | JP.Parse_error e ->
      raise (Jobpack_error (Invalid_jobdict (JP.string_of_error e)))
    | JC.Json_conv_error e ->
      raise (Jobpack_error (Invalid_jobdict (JC.string_of_error e)))

let pREFIX = "prefix"
let oWNER  = "owner"
let wORKER = "worker"
let iNPUTS = "inputs"
let pIPELINE = "pipeline"
let jobdict_from_json dict =
  List.iter (fun key ->
               if not (List.mem_assoc key dict)
               then raise (Jobpack_error (Missing_jobdict_key key))
            ) [pREFIX; oWNER; wORKER; pIPELINE; iNPUTS];
  let name   = JC.to_string (List.assoc pREFIX dict) in
  let owner  = JC.to_string (List.assoc  oWNER dict) in
  let worker = JC.to_string (List.assoc wORKER dict) in
  let inputs = List.map P.input_of_json (JC.to_list (List.assoc iNPUTS dict)) in
  let pipeline = P.pipeline_of_json (List.assoc pIPELINE dict) in
  { name; owner; worker; inputs; pipeline }

let jobdict_of hdr pack =
  jobdict_from_json (json_jobdict_of hdr pack)

(* job environment utilities *)

let json_jobenvs_of hdr pack =
  let jobenvs_len = hdr.jobhome_ofs - hdr.jobenvs_ofs in
  try JP.of_substring pack hdr.jobenvs_ofs jobenvs_len
  with JP.Parse_error e ->
    raise (Jobpack_error (Invalid_jobenvs (JP.string_of_error e)))

let jobenvs_of_json jobenvs =
  try
    let envs = JC.object_table_to_list (JC.to_object_table jobenvs) in
    List.map (fun (k, v) -> k, JC.to_string v) envs
  with JC.Json_conv_error e ->
    raise (Jobpack_error (Invalid_jobenvs (JC.string_of_error e)))

let jobenvs_of hdr pack =
  jobenvs_of_json (json_jobenvs_of hdr pack)

(* jobdata utilities *)

let jobdata_of hdr pack =
  String.sub pack hdr.jobdata_ofs (String.length pack - hdr.jobdata_ofs)

(* jobpack creation utilities *)

let zip_from_file f =
  let tf = F.temp_file "ocamljob" "" in
  let outz = Z.open_out tf in
  Z.copy_file_to_entry f outz (F.basename f);
  Z.close_out outz;
  let zip = U.contents_of_file tf in
  Unix.unlink tf;
  zip

let make_jobdict_json ~name ~owner ~worker ~pipeline ~inputs =
  let i = List.map P.json_of_input inputs in
  let n = pREFIX, J.String name in
  let o = oWNER, J.String owner in
  let w = wORKER, J.String worker in
  let p = pIPELINE, P.json_of_pipeline pipeline in
  let i = iNPUTS, J.Array (Array.of_list i) in
  J.Object (Array.of_list [n; o; w; p; i])

let make_jobpack ?(envs=[]) ?(jobdata="")
    ~name ~owner ~worker ~pipeline inputs =
  let jobdict = J.to_string (make_jobdict_json ~name ~owner
                               ~worker ~pipeline ~inputs) in
  let jobhome = zip_from_file worker in
  let envarr  = List.map (fun (k, v) -> k, J.String v) envs in
  let jobenvs = J.to_string (J.Object (Array.of_list envarr)) in

  let dict_len = String.length jobdict in
  let envs_len = String.length jobenvs in
  let home_len = String.length jobhome in
  let data_len = String.length jobdata in

  let ofs = ref hDR_LEN in
  let pack = String.create (!ofs + dict_len + envs_len + home_len + data_len) in
  put_int16 mAGIC pack 0;
  put_int16 vERSION pack 2;

  put_int32 hDR_LEN pack 4;
  String.blit jobdict 0 pack !ofs dict_len;
  ofs := !ofs + dict_len;

  put_int32 !ofs pack 8;
  String.blit jobenvs 0 pack !ofs envs_len;
  ofs := !ofs + envs_len;

  put_int32 !ofs pack 12;
  String.blit jobhome 0 pack !ofs home_len;
  ofs := !ofs + home_len;

  put_int32 !ofs pack 16;
  String.blit jobdata 0 pack !ofs data_len;

  pack
