LIB = odisco

OBJ_DIR = _build/lib

LIB_INSTALLS = \
	$(OBJ_DIR)/disco.a $(OBJ_DIR)/disco.cma $(OBJ_DIR)/disco.cmxa \
	$(OBJ_DIR)/task.mli $(OBJ_DIR)/task.cmi \
	$(OBJ_DIR)/worker.mli $(OBJ_DIR)/worker.cmi

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

install_test:
	make -C tests
