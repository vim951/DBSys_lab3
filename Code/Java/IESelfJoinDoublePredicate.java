package iterator;

import java.io.IOException;
import java.util.ArrayList;

import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.Tuple;
import index.IndexException;


public class IESelfJoinDoublePredicate extends Iterator {
	
	private static final TupleOrder ASCENDING = new TupleOrder(TupleOrder.Ascending);
	private static final TupleOrder DESCENDING = new TupleOrder(TupleOrder.Descending);
	
	private final ArrayList<Tuple> join_result;
	private int current_index;

	/**
	 *
	 * @param in1 				Attribute types of the 1st iterator
	 * @param len_in1			Size of in1
	 * @param t1_str_sizes		Max sizes of strings in the 1st iterator
	 * @param in2				Attribute types of the 2nd iterator
	 * @param len_in2			Size of in2
	 * @param t2_str_sizes		Max sizes of strings in the 2nd iterator
	 * @param amt_of_mem		Amount of memory
	 * @param am1				1st iterator
	 * @param am2				2nd iterator
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
	public IESelfJoinDoublePredicate(
						AttrType[]    	in1,    
			   			int     		len_in1,           
			   			short[]   		t1_str_sizes,
			   			AttrType[]    	in2,         
			   			int     		len_in2,           
			   			short[]   		t2_str_sizes,   
			   			int     		amt_of_mem,        
			   			Iterator     	am1,
			   			Iterator     	am2,    
			   			CondExpr[] 		filter,
			   			FldSpec[]  		proj_list,
			   			int   			n_out_flds,
			   			boolean 		memoryOptim
			   			)
			   			throws UnknowAttrType, LowMemException, JoinsException, Exception{
		
		//==================Whether we use the entire table or just one column per iterator===================
		
		int column_offset_1, column_offset_2;
		if(memoryOptim) {
			column_offset_1 = 1;
			column_offset_2 = 2;
		}else {
			column_offset_1 = filter[0].operand1.symbol.offset;
			column_offset_2 = filter[1].operand1.symbol.offset;
		}
		
		//====================================================================================================
		
		//=================================Lines 1 to 5 of algo2 pseudo-code==================================
		
		Sort L1,L2;
		TupleOrder to1, to2;
		
		if (filter[0].op.attrOperator == AttrOperator.aopGE || filter[0].op.attrOperator == AttrOperator.aopGT) {
			to1 = ASCENDING;
		}else if(filter[0].op.attrOperator == AttrOperator.aopLE || filter[0].op.attrOperator == AttrOperator.aopLT) {
			to1 = DESCENDING;
		}else {
			throw new IncorrectOperatorException();
		}
		
		if (filter[1].op.attrOperator == AttrOperator.aopGE || filter[1].op.attrOperator == AttrOperator.aopGT) {
			to2 = DESCENDING;

		}else if(filter[1].op.attrOperator == AttrOperator.aopLE || filter[1].op.attrOperator == AttrOperator.aopLT) {
			to2 = ASCENDING;
		}else {
			throw new IncorrectOperatorException();
		}
		
		L1 = new Sort (in1, (short) len_in1, t1_str_sizes, am1, column_offset_1, to1, 0, amt_of_mem);
		L2 = new Sort (in2, (short) len_in2, t2_str_sizes, am2, column_offset_2, to2, 0, amt_of_mem);
		
		//====================================================================================================
		
		//=====================================Convert L1 and L2 to lists=====================================
		
		ArrayList<Tuple> list1 = new ArrayList<>();
		ArrayList<Tuple> list2 = new ArrayList<>();
		Tuple tuple;
		while ((tuple = L1.get_next()) != null)
		{	
			list1.add(new Tuple(tuple));
		}
		while ((tuple = L2.get_next()) != null)
		{	
			list2.add(new Tuple(tuple));
		}
		
		L1.close();
		L2.close();
		
		am1.close();
		am2.close();
		
		//====================================================================================================
		
		//=================================Lines 6 to 7 of algo 2 pseudo-code=================================
		
		int n = list1.size();
		int[] P = new int[n]; //permutation array
		int[] B = new int[n]; //bit-array
		for (int i=0 ; i<n ; i++) {
			P[i]=0;
			B[i]=0;
		}
		
		for (int i=0 ; i<n ; i++) { //list1 loop
			for (int j=0 ; j<n ; j++) { //list2 loop
				if (TupleUtils.Equal(list1.get(i), list2.get(j), in1, len_in1)) {
					P[i] = j;
				}
			}
		}
		
		//====================================================================================================
		
		//====================================Line 8 of algo 2 pseudo-code====================================
		
		join_result = new ArrayList<>();
		current_index = 0;
		
		//====================================================================================================
		
		//================================Lines 9 to 10 of algo 2 pseudo-code=================================
		
		int eqOff;
		if ((filter[0].op.attrOperator == AttrOperator.aopGE || filter[0].op.attrOperator == AttrOperator.aopLE) && (filter[1].op.attrOperator == AttrOperator.aopGE || filter[1].op.attrOperator == AttrOperator.aopLE)) {
			eqOff = 0;
		}else {
			eqOff = 1;
		}
		
		//====================================================================================================
		
		//================================Lines 11 to 16 of algo 2 pseudo-code================================

		int pos;
		Tuple JTuple = new Tuple();
		AttrType[] JTypes = new AttrType[n_out_flds];
		TupleUtils.setup_op_tuple(JTuple, JTypes, in1, len_in1, in1, len_in1, t1_str_sizes, t1_str_sizes, proj_list, n_out_flds);

		
		for (int i=0 ; i<n ; i++) {
			pos = P[i];
			B[pos] = 1;
			for (int j = pos + eqOff ; j<n ; j++) {
				if (B[j]==1) {
					Projection.Join(list1.get(j), in1, list1.get(P[i]), in2, JTuple, proj_list, n_out_flds);
					join_result.add(new Tuple(JTuple));
				}
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
