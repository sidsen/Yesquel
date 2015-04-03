%{
#include <stdio.h>
#include <ctype.h>
#include "newconfig.h"

int yylex(void);
int yyerror(char const *);
extern int yylineno;

GroupConfig *currgroup;
HostConfig *currhost;
ConfigState *parser_cs;

%}

%start spec

%union {
   int ival;
   double dval;
   char  *sval;
}

%token <ival> T_INT
%token <ival> T_NGROUPS T_CONTAINER_ASSIGN_METHOD T_CONTAINER_ASSIGN_PARM T_GROUP T_NSITES T_NSERVERS T_STRIPE_METHOD T_STRIPE_PARM T_SITE T_SERVER T_HOST T_PORT T_LOGFILE T_STOREDIR T_BEGIN T_END T_PREFER_IP_PREFIX
%token <dval> T_FLOAT
%token <sval> T_STR

%%


spec		:	/* empty */
		|	spec specitem
		;

specitem	:	T_NGROUPS T_INT { parser_cs->setNgroups($2); }
		|	T_CONTAINER_ASSIGN_METHOD T_INT { parser_cs->setContainerAssignMethod($2); }
		|	T_CONTAINER_ASSIGN_PARM T_INT { parser_cs->setContainerAssignParm($2); }
		|	T_PREFER_IP_PREFIX T_STR { parser_cs->setPreferredIPPrefix($2); }
		|	group { parser_cs->addGroup(currgroup); }
		|	host { parser_cs->addHost(currhost); }
		
		;

group	:	T_GROUP T_INT { currgroup = new GroupConfig(); currgroup->id=$2; }
			T_BEGIN groupbody T_END
		;
		
host		:	T_HOST T_STR { currhost = new HostConfig();
				       currhost->hostname=$2;
				       currhost->port=0; }
			T_BEGIN hostbody T_END
		|	T_HOST T_STR T_PORT T_INT { currhost = new HostConfig();
						    currhost->hostname=$2;
						    currhost->port=$4; }
			T_BEGIN hostbody T_END
		;

groupbody	:	/* empty */
		|	groupbody citem
		;

hostbody	:	/* empty */
		|	hostbody hitem
		;

citem		:	T_NSITES T_INT { currgroup->nsites = $2; }
		|	T_NSERVERS T_INT { currgroup->nservers = $2; }
		|	T_STRIPE_METHOD T_INT { currgroup->stripeMethod = $2; }
		|	T_STRIPE_PARM T_INT { currgroup->stripeParm = $2; }
		|	T_SITE T_INT T_SERVER T_INT T_HOST T_STR T_PORT T_INT { currgroup->addSiteServer($2,$4,$6,$8, parser_cs->PreferredPrefix16); }
		|	T_SITE T_INT T_SERVER T_INT T_HOST T_STR { currgroup->addSiteServer($2,$4,$6,0, parser_cs->PreferredPrefix16); }
		;

hitem		:	T_LOGFILE T_STR { currhost->logfile = $2; }
		|	T_STOREDIR T_STR { currhost->storedir = $2; }
		;

%%

int yyerror(char const *msg){
   fprintf(stderr, "line %d: %s\n", yylineno, msg);
   return 1;
}

