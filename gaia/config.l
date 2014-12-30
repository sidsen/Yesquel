%option yylineno

%{
#include <string.h>
#include <math.h>
#include "config.tab.h"
#define YY_NO_UNISTD_H
#include <io.h>
#define isatty _isatty
%}

DIGIT	[0-9]
ID	[a-z_][a-z0-9_]*

%%

"{"      {  return T_BEGIN; }
"}"      {  return T_END; }

{DIGIT}+ {  yylval.ival = atoi(yytext); return T_INT; }

{DIGIT}+"."{DIGIT}* {  yylval.dval = atof(yytext); return T_FLOAT; }

ngroups			{ return T_NGROUPS; }
container_assign_method	{ return T_CONTAINER_ASSIGN_METHOD; }
container_assign_parm	{ return T_CONTAINER_ASSIGN_PARM; }
group			{ return T_GROUP; }
nsites			{ return T_NSITES; }
nservers		{ return T_NSERVERS; }
stripe_method		{ return T_STRIPE_METHOD; }
stripe_parm		{ return T_STRIPE_PARM; }
site			{ return T_SITE; }
server			{ return T_SERVER; }
host			{ return T_HOST; }
port			{ return T_PORT; }
logfile			{ return T_LOGFILE; }
storedir		{ return T_STOREDIR; }
prefer_ip_prefix        { return T_PREFER_IP_PREFIX; }

{ID}        { fprintf(stderr, "line %d: unrecognized token %s\n", yylineno, yytext); }


\"[^"]*\"      { int mylen = strlen(yytext);
                 yylval.sval = new char[mylen-1];
                 strncpy(yylval.sval, yytext+1, mylen-2); // skip ""'s
                 yylval.sval[mylen-2] = 0;
                 return T_STR; }

"#"[^\n]*    /* eat up comments */
[ \t\n]+       /* eat up whitespace */

.           printf( "Unrecognized character: %s\n", yytext );

%%


int yywrap(void){ return 1; }
