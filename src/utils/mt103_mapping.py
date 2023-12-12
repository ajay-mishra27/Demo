
#Will take from the database
mappingJson ={
"block1" : {"line_val":"{1:<config_block1>}","line_replace":"config_block1"},
"block2" : {"line_val":"{2:I103<sw_id><priority>}","line_replace":"sw_id,priority"},
"block3" : {"line_val":"{3:{108:<block_id>}{121:<block_id1>}}","line_replace":"block_id,block_id1"},
"block4" : {"line_val":"""{4:
:20:<reference_number>
:23B:CRED
:32A:<valuedate><currency><amount>
:50k:/<acct_nbr>
<account_name>
<address>
<city><state><zipcode>
:53B:/<number>
:57A:<block4_id>
:59:/<account_number>
<name>
<address1>
<address2>
:70:<line1>
<line2>
<line3>
<line4>
:71A:<charges>
-}
""","line_replace":"reference_number,valuedate,currency,amount,acct_nbr,account_name,address,city,state,zipcode,number,block4_id,account_number,name,address1,address2,line1,line2,line3,line4,charges"}
}