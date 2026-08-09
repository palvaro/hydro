# Do not throw away output!

A common mistake you always seem to make when running command-line programs (which may run for minutes or even hours) is piping through a command (e.g. grep) which THROWS AWAY most of the output.  Then, if your hunch was wrong about what to look for, you run the WHOLE PROGRAM again, and again, and again.......

Don't do this.  Whenever you run a program at the command line, redirect its output to a file.  Run whatever command (grep, head, awk etc) on that file *as many times as you need to explore the outputs!* Run the program only once (unless you change the program).

This will save us many many hours.  Don't be a dummy.

Delete the output files when you are done.

Note also that mac has no "timeout" command. Use other tactics.