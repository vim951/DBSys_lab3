package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.Tuple;
import index.IndexException;


public class IEJoinDoublePredicate extends Iterator {
	
	private static final TupleOrder ASCENDING = new TupleOrder(TupleOrder.Ascending);
	private static final TupleOrder DESCENDING = new TupleOrder(TupleOrder.Descending);
	
	private final ArrayList<Tuple> join_result;
	private int current_index;

	/**
	 *
	 * @param in1 				Attribute types of X
	 * @param len_in1			Size of in1
	 * @param t1_str_sizes		Max sizes of strings in X
	 * @param in2				Attribute types of Y
	 * @param len_in2			Size of in2
	 * @param t2_str_sizes		Max sizes of strings in Y
	 * @param amt_of_mem		Amount of memory
	 * @param am11				1st iterator
	 * @param am12				2nd iterator
	 * @param am21				3rd iterator (=am11)
	 * @param am22				4th iterator (=am12)
	 * @param outFilter			Conditional expression with operators
	 * @param proj_list			Indicate what input fields go where in the output tuple
	 * @param n_out_flds		Size of proj_list
	 * @throws UnknowAttrType 	from lower layers
	 * @throws LowMemException 	from lower layers
	 * @throws JoinsException 	from lower layers
	 * @throws Exception 		from lower layers
	 */
	public IEJoinDoublePredicate(
						AttrType[]    	in1,    
			   			int     		len_in1,           
			   			short[]   		t1_str_sizes,
			   			AttrType[]    	in2,         
			   			int     		len_in2,           
			   			short[]   		t2_str_sizes,   
			   			int     		amt_of_mem,        
			   			Iterator     	am11,
			   			Iterator     	am12,
			   			Iterator     	am21,
			   			Iterator     	am22,
			   			CondExpr[] 		outFilter,      
			   			FldSpec[]  		proj_list,
			   			int   			n_out_flds
			   			)
			   			throws UnknowAttrType, LowMemException, JoinsException, Exception, TupleUtilsException{
		
		//=======================================Lines 3 to 6 of algo 1=======================================
		
		TupleOrder to11, to12, to21, to22;
		
		if (outFilter[0].op.attrOperator == AttrOperator.aopGE || outFilter[0].op.attrOperator == AttrOperator.aopGT) {
			to11 = DESCENDING;
			to12 = DESCENDING;			
		}else if(outFilter[0].op.attrOperator == AttrOperator.aopLE || outFilter[0].op.attrOperator == AttrOperator.aopLT) {
			to11 = ASCENDING;
			to12 = ASCENDING;		
		}else {
			throw new IncorrectOperatorException();
		}
		
		if (outFilter[1].op.attrOperator == AttrOperator.aopGE || outFilter[1].op.attrOperator == AttrOperator.aopGT) {
			to21 = ASCENDING;
			to22 = ASCENDING;		
		}else if(outFilter[1].op.attrOperator == AttrOperator.aopLE || outFilter[1].op.attrOperator == AttrOperator.aopLT) {
			to21 = DESCENDING;
			to22 = DESCENDING;	
		}else {
			throw new IncorrectOperatorException();
		}
		
		Sort L11,L12,L21,L22;
		L11 = new Sort (in1, (short) len_in1, t1_str_sizes, am11, outFilter[0].operand1.symbol.offset, to11, 0, amt_of_mem);
		L12 = new Sort (in2, (short) len_in2, t2_str_sizes, am12, outFilter[0].operand2.symbol.offset, to12, 0, amt_of_mem);
		L21 = new Sort (in1, (short) len_in1, t1_str_sizes, am21, outFilter[1].operand1.symbol.offset, to21, 0, amt_of_mem);
		L22 = new Sort (in2, (short) len_in2, t2_str_sizes, am22, outFilter[1].operand2.symbol.offset, to22, 0, amt_of_mem);
		
		//====================================================================================================
		
		//===============================Convert L11, L12, L21 and L22 to lists===============================
		
		ArrayList<Tuple> list11 = new ArrayList<>();
		ArrayList<Tuple> list12 = new ArrayList<>();
		ArrayList<Tuple> list21 = new ArrayList<>();
		ArrayList<Tuple> list22 = new ArrayList<>();
		Tuple tuple;
		while ((tuple = L11.get_next()) != null)
		{	
			list11.add(new Tuple(tuple));
		}
		while ((tuple = L12.get_next()) != null)
		{	
			list12.add(new Tuple(tuple));
		}
		while ((tuple = L21.get_next()) != null)
		{	
			list21.add(new Tuple(tuple));
		}
		while ((tuple = L22.get_next()) != null)
		{	
			list22.add(new Tuple(tuple));
		}
		
		//====================================================================================================
		
		//====================================Lines 7, 8 and 11 of algo 1=====================================
		
		int m = list11.size();
		int n = list12.size();
		
		int[] P1 = new int[m];
		int[] P2 = new int[n];
		int[] B2 = new int[n];
		for (int i=0 ; i<m ; i++) {
			P1[i]=0;
		}
		for (int i=0 ; i<n ; i++) {
			P2[i]=0;
			B2[i]=0;
		}
		
		for (int i=0 ; i<m ; i++) { //list11 loop
			for (int j=0 ; j<m ; j++) { //list21 loop
				if (TupleUtils.Equal(list11.get(i), list21.get(j), in1, len_in1)) {
					P1[i] = j;
				}
			}
		}
		for (int i=0 ; i<n ; i++) { //list12 loop
			for (int j=0 ; j<n ; j++) { //list22 loop
				if (TupleUtils.Equal(list12.get(i), list22.get(j), in2, len_in2)) {
					P2[i] = j;
				}
			}
		}
		
		//====================================================================================================
		
		//======================================Lines 9 and 10 of algo 1======================================
		
		int[] O1 = new int[m];
		int i1 = 0;
		int i2 = 0;
		while (i1<m) {
			while(i2<n && TupleUtils.CompareTupleWithTuple(in1[outFilter[0].operand1.symbol.offset-1], list11.get(i1), outFilter[0].operand1.symbol.offset, list12.get(i2), outFilter[0].operand2.symbol.offset)>=0) {
				i2++;
			}
			O1[i1++] = i2;
		}
		
		int[] O2= new int[m];
		i1 = 0;
		i2 = 0;
		while (i1<m) {
			while(i2<n && TupleUtils.CompareTupleWithTuple(in1[outFilter[1].operand1.symbol.offset-1], list21.get(i1), outFilter[1].operand1.symbol.offset, list22.get(i2), outFilter[1].operand2.symbol.offset)>=0) {
				i2++;
			}
			O2[i1++] = i2;
		}
		
		//====================================================================================================
		
		//=========================================Line 12 of algo 1==========================================
		
		join_result = new ArrayList<>();
		current_index = 0;
		
		//====================================================================================================
		
		//=====================================Lines 13 and 14 of algo 1======================================
		
		int eqOff;
		if ((outFilter[0].op.attrOperator == AttrOperator.aopGE || outFilter[0].op.attrOperator == AttrOperator.aopLE) && (outFilter[1].op.attrOperator == AttrOperator.aopGE || outFilter[1].op.attrOperator == AttrOperator.aopLE)) {
			eqOff = 0;
		}else {
			eqOff = 1;
		}
		
		//====================================================================================================
		
		//======================================Lines 15 to 22 of algo 1======================================

		int off1, off2;
		Tuple JTuple = new Tuple();
		AttrType[] JTypes = new AttrType[n_out_flds];
		TupleUtils.setup_op_tuple(JTuple, JTypes, in1, len_in1, in1, len_in1, t1_str_sizes, t1_str_sizes, proj_list, n_out_flds);

		
		for (int i=0 ; i<m ; i++) {
			off2 = O2[i];
			for (int j=0 ; j<=Math.min(off2, m-1) ; j++) {
				B2[P2[j]] = 1;
			}
			off1 = O1[P1[i]];
			for (int k=off1+eqOff ; k<n ; k++) {
				if(B2[k]==1) {
					Projection.Join(list21.get(i), in1, list12.get(k), in2, JTuple, proj_list, n_out_flds);
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
