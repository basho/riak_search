%% Change the default index/field from this point forward.
-record(scope, {
          id=make_ref(),
          index=undefined,
          field=undefined,
          ops=[]
         }).

%% Take the union of all results from all branches.
-record(union, {
          id=make_ref(),
          ops=[]
         }).

%% Take the intersection of all results from all branches.
-record(intersection, { 
          id=make_ref(),
          ops=[] 
         }).

%% Run the specified results in a group, applying the default operation.
-record(group, { 
          id=make_ref(),
          ops=[] 
         }).

%% Invert the effect of these results. Not allowed for topmost thing.
-record(negation, { 
          id=make_ref(),
          op=[] 
         }).

%% Search in a range.
-record(range, { 
          id=make_ref(),
          from=undefined, 
          to=undefined 
         }).
-record(inclusive, { s=undefined }).
-record(exclusive, { s=undefined }).

%% Search on a specific term or phrase.
%% Args: [{boost, X}, {proximity, X}, {fuzzy, X}]
-record(string, { 
          id=make_ref(),
          s="Term/Phrase", 
          flags=[]
         }).
