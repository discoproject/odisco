LIB = odisco

OBJ_DIR = _build/lib

LIB_INSTALLS = \
	$(OBJ_DIR)/disco.a      $(OBJ_DIR)/disco.cma $(OBJ_DIR)/disco.cmxa \
	$(OBJ_DIR)/task.mli     $(OBJ_DIR)/task.cmi \
	$(OBJ_DIR)/worker.mli   $(OBJ_DIR)/worker.cmi \
	$(OBJ_DIR)/cfg.mli      $(OBJ_DIR)/cfg.cmi \
	$(OBJ_DIR)/pipeline.mli $(OBJ_DIR)/pipeline.cmi \
	$(OBJ_DIR)/jobpack.mli  $(OBJ_DIR)/jobpack.cmi \
	$(OBJ_DIR)/rest_api.mli $(OBJ_DIR)/rest_api.cmi

.PHONY: all clean install uninstall reinstall

all:
	ocamlbuild -use-ocamlfind all.otarget

clean:
	ocamlbuild -clean
	make -C tests clean

install:
	ocamlfind install $(LIB) META $(LIB_INSTALLS)

uninstall:
	ocamlfind remove $(LIB)

reinstall: uninstall install

install_test:
	make -C tests
