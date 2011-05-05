# This Makefile is used only for testing the installation.

OCAMLC = ocamlfind ocamlc -linkpkg -verbose -package odisco.disco
OCAMLOPT = ocamlfind ocamlopt -linkpkg -verbose -package odisco.disco

.PHONY: all byte native clean

all: byte native

byte:
	$(OCAMLC) -o test_install_task test_install_task.ml

native:
	$(OCAMLOPT) -o test_install_task.opt test_install_task.ml

clean:
	-rm -f test_install_task{,.cmi,.cmo,.cmx,.o,.opt}
