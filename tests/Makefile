# This Makefile is used only for testing the installation.

OCAMLC = ocamlfind ocamlc -linkpkg -verbose -package odisco.disco
OCAMLOPT = ocamlfind ocamlopt -linkpkg -verbose -package odisco.disco
TEST_TASK = word_count

.PHONY: all byte native clean

all: byte

wc_t.mli wc_t.ml: wc.atd
	atdgen -t $^

wc_b.mli wc_b.ml: wc.atd
	atdgen -b $^

byte: wc_t.mli wc_t.ml wc_b.mli wc_b.ml
	$(OCAMLC) -g -o $(TEST_TASK) $(TEST_TASK).ml

native:
	$(OCAMLOPT) -g -o $(TEST_TASK).opt $(TEST_TASK).ml

clean:
	-rm -f $(TEST_TASK){,.cmi,.cmo,.cmx,.o,.opt}
