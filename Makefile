LIB = odisco

OBJ_DIR = _build/lib

LIB_INSTALLS = \
	$(OBJ_DIR)/disco.{a,cma,cmxa} \
	$(OBJ_DIR)/task.{cmi,mli} \
	$(OBJ_DIR)/worker.{cmi,mli}

.PHONY: all clean install uninstall reinstall

all:
	ocamlbuild -use-ocamlfind all.otarget

clean:
	ocamlbuild -clean
	make -C tests clean

install: all
	ocamlfind install $(LIB) META $(LIB_INSTALLS)

uninstall:
	ocamlfind remove $(LIB)

reinstall: uninstall install
