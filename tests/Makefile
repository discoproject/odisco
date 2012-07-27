# This Makefile is used only for testing the installation.

OCAMLC = ocamlfind ocamlc -linkpkg -verbose -package odisco.disco
OCAMLOPT = ocamlfind ocamlopt -linkpkg -verbose -package odisco.disco
TEST_TASK = word_count

.PHONY: all byte native clean

all: byte native

byte:
	$(OCAMLC) -g -o $(TEST_TASK) $(TEST_TASK).ml

native:
	$(OCAMLOPT) -g -o $(TEST_TASK).opt $(TEST_TASK).ml

clean:
	-rm -f $(TEST_TASK){,.cmi,.cmo,.cmx,.o,.opt}
