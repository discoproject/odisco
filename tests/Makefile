# This Makefile is used only for testing the installation.

ATDGEN_SOURCES = wc.atd
ATDGEN_MODULES = wc_t.ml wc_b.ml
include Atdgen.mk
LINKPKGS = -package odisco.disco -package biniou -package atdgen
OCAMLC = ocamlfind ocamlc -linkpkg -verbose $(LINKPKGS)
OCAMLOPT = ocamlfind ocamlopt -linkpkg -verbose $(LINKPKGS)
TEST_TASK = word_count

.PHONY: all byte native clean

all: byte

byte: $(ATDGEN_OUTFILES)
	$(OCAMLC) -c wc_t.mli
	$(OCAMLC) -c wc_b.mli
	$(OCAMLC) -g -o $(TEST_TASK) $(ATDGEN_MODULES) $(TEST_TASK).ml

native: $(ATDGEN_OUTFILES)
	$(OCAMLOPT) -g -o $(TEST_TASK).opt $(TEST_TASK).ml

clean:
	-rm -f {$(TEST_TASK),wc_t,wc_b}{,.cmi,.cmo,.cmx,.o,.opt} $(TRASH) 
