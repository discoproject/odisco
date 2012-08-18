open Ocamlbuild_plugin

(* OCaml plugin to handle building tests that use atd *)

let atdgen = A "atdgen";;

dispatch begin function
  | After_rules ->
      (* Rules to generate atdgen-erated modules *)
      rule "atdgen : atd -> _t.mli & _t.ml"
        ~dep:"%.atd"
        ~prods:["%_t.mli"; "%_t.ml"]
        (fun env _build ->
          Cmd (S [atdgen; A "-t"; P (env "%.atd")]));
      rule "atdgen : atd -> _b.mli & _b.ml"
        ~dep:"%.atd"
        ~prods:["%_b.mli"; "%_b.ml"]
        (fun env _build ->
          Cmd (S [atdgen; A "-b"; P (env "%.atd")]));
      rule "atdgen : atd -> _j.mli & _j.ml"
        ~dep:"%.atd"
        ~prods:["%_j.mli"; "%_j.ml"]
        (fun env _build ->
          Cmd (S [atdgen; A "-j"; P (env "%.atd")]))
  | _ -> ()
end
