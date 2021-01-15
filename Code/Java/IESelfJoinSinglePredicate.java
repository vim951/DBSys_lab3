package iterator;

import java.io.IOException;
import java.util.ArrayList;

import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.Tuple;
import index.IndexException;


public class IESelfJoinSinglePredicate extends Iterator {
	
	private static final TupleOrder ASCENDING = new TupleOrder(TupleOrder.Ascending);
	private static final TupleOrder DESCENDING = new TupleOrder(TupleOrder.Descending);
	
	private final ArrayList<Tuple> join_result;
	private int current_index;

	/**
	 *
	 * @param in 				Attribute types of the iterator
	 * @param len_in			Size of in
	 * @param str_sizes			Max sizes of the strings in the iterator
	 * @param amt_of_mem		Amount of memory
	 * @param am				iterator
	 * @param filter			Conditional expression with operators
	 * @param proj_list			Indicate what input fields go where in the output tuple
	 * @param n_out_flds		Size of proj_list
	 * @param memory_optim		Whether we use the entire table (false) or just one column (true)
	 * 
	 * @throws UnknowAttrType 	from lower layers
	 * @throws LowMemException 	from lower layers
	 * @throws JoinsException 	from lower layers
	 * @throws Exception 		from lower layers
	 */
	public IESelfJoinSinglePredicate(
						AttrType[]    	in,    
			   			int     		len_in,           
			   			short[]   		str_sizes, 
			   			int     		amt_of_mem,        
			   			Iterator     	am,  
			   			CondExpr[] 		filter,  
			   			FldSpec[]  		proj_list,
			   			int   			n_out_flds,
			   			boolean 		memoryOptim
			   			)
			   			throws UnknowAttrType, LowMemException, JoinsException, Exception{
		
		//========================Whether we use the entire table or just one column==========================
		
		int column_offset;
		if(memoryOptim) {
			column_offset = 1;
		}else {
			column_offset = filter[0].operand1.symbol.offset;
		}
		
		//====================================================================================================
		
		//=================================Lines 1 to 5 of algo2 pseudo-code==================================
		
		TupleOrder tupleOrder;
		if (filter[0].op.attrOperator == AttrOperator.aopGE || filter[0].op.attrOperator == AttrOperator.aopGT) {
			tupleOrder = ASCENDING;
		}else if(filter[0].op.attrOperator == AttrOperator.aopLE || filter[0].op.attrOperator == AttrOperator.aopLT) {
			tupleOrder = DESCENDING;
		}else {
			throw new IncorrectOperatorException();
		}
		
		Sort L1 = new Sort (in, (short) len_in, str_sizes, am, column_offset, tupleOrder, 0, amt_of_mem);
		
		//====================================================================================================

		//========================================Convert L1 to a list========================================
		
		ArrayList<Tuple> list1 = new ArrayList<>();
		Tuple tuple;
		while ((tuple = L1.get_next()) != null)
		{	
			list1.add(new Tuple(tuple));
		}
		am.close();
		L1.close();
		
		//====================================================================================================
		
		//=================================Lines 9 to 10 of algo2 pseudo-code=================================
		
		int eqOff;
		if ((filter[0].op.attrOperator == AttrOperator.aopGE || filter[0].op.attrOperator == AttrOperator.aopLE)) {
			eqOff = 0;
		}else {
			eqOff = 1;
		}
		
		//====================================================================================================
		
		//=============Lines 11 to 16 of algo2 pseudo-code, adapted to the single predicate case==============

		join_result = new ArrayList<>();
		
		Tuple JTuple = new Tuple();
		AttrType[] JTypes = new AttrType[n_out_flds];
		TupleUtils.setup_op_tuple(JTuple, JTypes, in, len_in, in, len_in, str_sizes, str_sizes, proj_list, n_out_flds);
		
		for (int i=0; i<list1.size(); i++) {
			for (int j=0; j<i+eqOff; j++) {
					Projection.Join(list1.get(i), in, list1.get(j), in, JTuple, proj_list, n_out_flds);
					join_result.add(new Tuple(JTuple));
			}
		}
		
		//====================================================================================================
		
	}
	
	
  /**
   * Used to browse the join result
   */
	public Tuple get_next() {
		if(current_index < join_result.size()) {
			return join_result.get(current_index++);
		}else {
			return null;
		}
	}
	
  /**
   * Cleaning up
   *@exception JoinsException from lower layers
   *@exception SortException from lower layers
   *@exception IndexException from lower layers
   *@exception IOException from lower layers
   */
	public void close() throws JoinsException, SortException, IndexException, IOException {
		if (!closeFlag) {
			closeFlag = true;
		}

	}

	/**
	 * Function used for debug purposes
	 * @return the length of the result
	 */
	public int getSize() {
		return join_result.size();
	}

}
