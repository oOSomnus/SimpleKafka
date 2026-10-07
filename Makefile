.DEFAULT_GOAL := help

ACTIONS := setup doctor compile test step-test reference-test list demo reference-demo book
NUMBERS := 01 02 03 04 05 06 07 08 09 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28
ACTIONS_GIVEN := $(filter $(ACTIONS),$(MAKECMDGOALS))
NUMBERS_GIVEN := $(filter $(NUMBERS),$(MAKECMDGOALS))
UNKNOWN_GIVEN := $(filter-out $(ACTIONS) $(NUMBERS),$(MAKECMDGOALS))

ifneq ($(strip $(UNKNOWN_GIVEN)),)
$(error Unsupported make goal(s): $(UNKNOWN_GIVEN))
endif
ifneq ($(strip $(MAKECMDGOALS)),)
ifeq ($(strip $(ACTIONS_GIVEN)),)
$(error A command is required; run 'make help')
endif
ifneq ($(words $(ACTIONS_GIVEN)),1)
$(error Use exactly one command goal)
endif
ifneq ($(words $(NUMBERS_GIVEN)),0)
ifneq ($(words $(NUMBERS_GIVEN)),1)
$(error Use at most one numeric step argument)
endif
ifneq ($(strip $(NUMBERS_GIVEN)),)
ifeq ($(filter test step-test reference-test,$(ACTIONS_GIVEN)),)
$(error A numeric argument is valid only with test, step-test, or reference-test)
endif
ifneq ($(strip $(STEP)),)
$(error Do not combine a numeric goal and STEP=)
endif
endif
endif
endif
.PHONY: help setup doctor compile test step-test reference-test list demo reference-demo book $(NUMBERS)
help:
	@printf '%s\n' 'simpleKafka course commands:' '  make setup                 Prepare pinned JUnit/Tectonic tools under .tools/' '  make doctor                Check Java, Make, PDF tools, and CJK fonts' '  make compile               Compile student skeleton and all tests' '  make test [N|STEP=N]       Cumulative student tests through N (default: all)' '  make step-test N           Run only step N; diagnostic, not graduation' '  make reference-test [N]    Cumulative tests against isolated reference answers' '  make list                  List all 28 steps and prerequisites' '  make demo                  Run the student TCP/failover scenario' '  make reference-demo        Run the completed reference scenario' '  make book                  Compile docs/book/simpleKafka.tex to PDF'

setup:
	@bash scripts/setup.sh

doctor:
	@bash scripts/course.sh doctor '' '$(MAKECMDGOALS)'

compile:
	@bash scripts/course.sh compile '' '$(MAKECMDGOALS)'

test:
	@bash scripts/course.sh test '$(STEP)' '$(MAKECMDGOALS)'

step-test:
	@bash scripts/course.sh step-test '$(STEP)' '$(MAKECMDGOALS)'

reference-test:
	@bash scripts/course.sh reference-test '$(STEP)' '$(MAKECMDGOALS)'

list:
	@bash scripts/course.sh list '' '$(MAKECMDGOALS)'

demo:
	@bash scripts/course.sh demo '' '$(MAKECMDGOALS)'

reference-demo:
	@bash scripts/course.sh reference-demo '' '$(MAKECMDGOALS)'

book:
	@bash scripts/book.sh

$(NUMBERS):
	@:
