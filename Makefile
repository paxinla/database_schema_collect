pkg_version := $(shell sed -n '/version=/p' setup.py | sed "s/[',]//g" | awk -F'=' '{print $$2}')
py_version := $(shell python -V 2>&1 | awk '{print $$2}' | awk -F'.' '{print $$1"."$$2}')
target_wheel := database_schema_collect-${pkg_version}-py${py_version}-none-any.whl

gen_target_whl :
	python setup.py bdist_wheel
	$(info generate dist/${target_wheel})

install:
	pip install dist/${target_wheel}
	$(info install ${target_wheel})

clean:
	-rm -rf build
	-rm -rf dist
	-rm -rf database_schema_collect.egg-info
