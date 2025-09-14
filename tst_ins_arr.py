#
# tst_ins_arr : what happens when a large set of data gets inserted..
#
# main chase is still : RoundTrips, RTs
#
# compare (evidently) row-by-row with execute-many. (done in tst_ins_label.py)
# then test exec- and exec-many, and other varieties.
#
# setup:
#   tst_ins_labels.sql (re-use previous test..?)
#   t1.sql (create more simple table)
#


# take this one Early.
# from  duration      import *

print ( ' ---- tst_netw.py --- ' )

print ( ' ---- tst_netw.py first do imports ..--- ' )

import    time
import    string
import    random 
from      datetime  import  datetime

# local utilities, keep/cp in same directory for now...
# from  duration      import *
from  prefix        import *
from  ora_login     import *

pp    ()
pp    ( ' ----- imports done, next def functions, global, constants ---- ' )
pp    ()

# global (list) variable to use at insert
itl_list         = []             # empty list for records, to be filled, inserted, re-used
itl_list_max_len = 1000000         # trigger SQL-Insert, and use for prefecth/arraysizes

# need an insert-stmnt, and an array to give to insert
itl_list_sql_ins = """        
INSERT /* list  */ into tst_item_label
       (   itm_id,   src_id,   label,   label_score,   label_comment )
VALUES ( :bitm_id, :bsrc_id, :blabel, :blabel_score, :blabel_comment )
""" 
# note:  bind vars: 5x input, later, add  1x output (returning)

def f_ins_label_list2db ( ):

  global itl_list       # allow this function to modify the global list

  if ( len ( itl_list )  < 1 ):
    pass
    # pp ( ' list2db, empty list, no action ' )
    return 0

  itl_cur = the_conn.cursor ()

  # use the (global-)constant for SQL, and the global itl_list for data
  itl_cur.executemany ( itl_list_sql_ins, itl_list )

  # re-initialize the list..
  itl_list = []

  return itl_cur.rowcount   # report the nr of records inserted

# function to fill the list (and check max-len)

def f_ins_label_add2list ( itm_id, src_id, label, label_score, label_comment ):

  global itl_list       # allow this function to modify the global list

  itl_list.append ( ( itm_id
                    , src_id   
                    , label
                    , label_score
                    , label_comment ) )

  if ( len ( itl_list ) > itl_list_max_len ):  # time to transfer the list?

    n_2db = f_ins_label_list2db ()

  else:

    n_2db = 0   # no records got inserted to RDBMS on this pass

  # inserted, if lenght..

  return n_2db  #  nr of records added to db.


#         ------ start of MAIN ----------

pp    ( ' ' )
pp    ( '--------- functions defined, start of MAIN..  ---------- ' )
pp    ( ' ' ) 

the_conn = ora_logon ()

# conneciton open, read work here


n_sec_test = 10

pp ( ' ' )
pp   ( 'Start: run test list for ', n_sec_test, ' sec.' )
pp ( ' ' )

# now loop for n_sec.., use list to buffer
n_counter = int ( 0 )
n_2db     = 0
start_t   = datetime.now().timestamp()        # time.time()
end_t     = start_t + n_sec_test

itm_id = 1
src_id = 1


while datetime.now().timestamp() < end_t:   # time.time() < end_t:

  label         = 'label t2 add2list' + '_' + str ( n_counter )
  label_comment = 'comment  label2list=' + str(n_counter) + ''.join(random.choices(string.ascii_letters, k=100))
  label_score   = 1 - ( 1 / itm_id )

  n_2db = n_2db + f_ins_label_add2list ( itm_id, src_id, label, label_score, label_comment )

  n_counter = n_counter + 1

# end while loop

# Important: empty out the list, keep total
n_2db = n_2db + f_ins_label_list2db ()

the_conn.commit ()

pp    ( ' ' )
pp    ( 'done, nr loops:', n_counter, ', list2db speed:', round ( n_counter / n_sec_test, 2) , ' records/sec.' )
pp    ( ' ' )


# report out the effort done.

ora_sess_info ( the_conn )

ora_time_spent ( the_conn ) 

pp    ( ' ' )  
pp    ( '---- end of ... ---- ' )
pp    ( ' ' )

