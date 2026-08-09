# Make sure to use command-line escapes correctly

A common mistake you always seem to make is calling the command-line with unescaped bangs.  For example:

grep "nondet!" 

If you do this, you will get hung up.  Always be sure to carefully escape the '!' character.