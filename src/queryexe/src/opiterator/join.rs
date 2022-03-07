use super::{OpIterator, TupleIterator};
use common::{CrustyError, Field, SimplePredicateOp, TableSchema, Tuple};
use std::collections::HashMap;

/// Compares the fields of two tuples using a predicate. (You can add any other fields that you think are neccessary)
/// all variable names still the same 
pub struct JoinPredicate {
    /// Operation to comapre the fields with.
    op: SimplePredicateOp,
    /// Index of the field of the left table (tuple).
    left_index: usize,
    /// Index of the field of the right table (tuple).
    right_index: usize,
    curr_left_tuple: Option<Tuple>
}


impl JoinPredicate {
    /// Constructor that determines if two tuples satisfy the join condition.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation to compare the two fields with.
    /// * `left_index` - Index of the field to compare in the left tuple.
    /// * `right_index` - Index of the field to compare in the right tuple.
    fn new(op: SimplePredicateOp, left_index: usize, right_index: usize) -> Self {
            JoinPredicate {
                op: op,
                left_index: left_index,
                right_index: right_index,
                curr_left_tuple: None,
            }
    }

    fn compare_left_right(&self, right_tuple: &Tuple) -> bool {
        let right_field = right_tuple.get_field(self.right_index).unwrap();
        let left_tuple = self.curr_left_tuple.as_ref().unwrap();
        let left_field = left_tuple.get_field(self.left_index).unwrap();

        self.op.compare(left_field, right_field)
    }
}


/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct Join {
    /// Join condition.
    predicate: JoinPredicate,
    /// Left child node.
    left: Box<dyn OpIterator>,
    /// Right child node.
    right: Box<dyn OpIterator>,
    /// Schema of the result.
    schema: TableSchema,
    open: bool,
    first: bool
}

impl Join {
    /// Join constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        let jp = JoinPredicate::new(op,left_index,right_index);
        let rs_schema = right_child.get_schema();
        let mut new = Join {
            predicate: jp,
            schema: left_child
                .get_schema()
                .clone()
                .merge(&rs_schema)
                .clone(),
            left: left_child,
            right: right_child,
            open: false,
            first: true,
        };
        let open = new.left.open();
        let next = new.left.next().unwrap();
        new.predicate.curr_left_tuple = next;
        let rewind = new.left.rewind();
        let close = new.left.close();
        return new;
    }
}

impl OpIterator for Join {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        match (self.left.open(), self.right.open()) {
            (Ok(()), Ok(())) => {
                Ok(())
            }
            _ => {
                Err(CrustyError::CrustyError(String::from("can't open children")))
            }
        }
    }

    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if self.open {
            while let Some(left_t) = self.predicate.curr_left_tuple.as_ref() {
                while let Some(t) = self.right.next()? {
                    let res = self.predicate.compare_left_right(&t);
                    if res {
                        return Ok(Some(left_t.merge(&t)));
                    }
                }
                if self.first {
                    self.first = false;
                    self.left.next()?;
                }
                // update curr_left_tuple and rewind right child
                match self.left.next()? {
                    Some(left_t) => {
                        self.right.rewind()?;
                        self.predicate.curr_left_tuple = Some(left_t);
                    }
                    None => {
                        return Ok(None);
                    }
                }
            }
            return Err(CrustyError::CrustyError(String::from("next failed")))
        }
        else {
            return Err(CrustyError::CrustyError(String::from("self isn't opened")))
        }

    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if self.open == false{
            panic!("it's already closed")
        }
        self.open = false;
        let right = self.right.close();
        match right {
            Ok(()) => {
                let left = self.left.close();
                match left {
                    Ok(()) => Ok(()),
                    Err(e) => return Err(CrustyError::CrustyError(String::from("error on left close")))
                }
            },
            Err(e) => return Err(CrustyError::CrustyError(String::from("error on right close")))
        }
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if self.open {
            let left = self.left.rewind();
            match left {
                Ok(()) => {
                    let right = self.right.rewind();
                    match right {
                        Ok(()) => {
                            let close = self.close();
                            match close {
                                Ok(()) => self.open(),
                                Err(e) => return Err(CrustyError::CrustyError(String::from("error on close")))
                            }
                        }
                        Err(e) => return Err(CrustyError::CrustyError(String::from("error on right rewind")))
                    }
                },
                Err(e) => return Err(CrustyError::CrustyError(String::from("error on left rewind")))
            }

        }
        else {
            return Err(CrustyError::CrustyError(String::from("op hasn't been opened")))
        }

    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    left: Box<dyn OpIterator>,
    right: Box<dyn OpIterator>,

    schema: TableSchema,
    hash_table: HashMap<Field, (Vec<Tuple>, usize)>, //vector of tuples
    /// Boolean determining if iterator is open.
    open: bool,
    right_index: usize,
    left_index: usize
}

impl HashEqJoin {
    /// Constructor for a hash equi-join operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        let mut new = HashEqJoin {
            schema: left_child
                .get_schema()
                .clone()
                .merge(&right_child.get_schema())
                .clone(),
            left: left_child,
            right: right_child,
            right_index: right_index,
            left_index: left_index,
            open: false,
            hash_table: HashMap::new(),
        };
        new.load_map();
        new
    }


    pub fn load_map(&mut self) -> Result<(), CrustyError> {
        let open = self.open();
        match open {
            Ok(()) => {
                while let Some(left_t) = self.left.next()? {
                    let left_field = left_t.get_field(self.left_index).unwrap();
                    let (ref mut vec, _) = self
                        .hash_table
                        .entry(left_field.clone())
                        .or_insert((Vec::new(), 0));
                    vec.push(left_t.clone());
                }
                return self.close();
            },
            Err(e) => return Err(CrustyError::CrustyError("open children failed".to_string()))
        }
    }
}

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        match (self.left.open(), self.right.open()) {
            (Ok(_), Ok(_)) => {
                return Ok(());
            }
            _ => {
                return Err(CrustyError::CrustyError("open children failed".to_string()));
            }
        }
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        while let Some(right_t) = self.right.next()? {
            match self.hash_table.get_mut(right_t.get_field(self.right_index).unwrap()) {
                Some((vec, i)) => {
                    if *i >= vec.len(){
                        *i = 0;
                    }
                    else {
                        let ret = Some(vec[*i].merge(&right_t));
                        *i += 1;
                        return Ok(ret);
                    } 
                }
                None => {}
            }
        }
        return Ok(None);
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.open = false;
        let right = self.right.close();
        match right {
            Ok(()) => {
                let left = self.left.close();
                match left {
                    Ok(()) => Ok(()),
                    Err(e) => return Err(CrustyError::CrustyError(String::from("error on left close")))
                }
            },
            Err(e) => return Err(CrustyError::CrustyError(String::from("error on right close")))
        }
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if self.open {
            self.left.rewind()?;
            self.right.rewind()?;
            self.load_map()?;
            self.close()?;
            return self.open();
        }
        else {
            return Err(CrustyError::CrustyError(String::from("wasn't open")))
        }
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;
    use common::testutil::*;

    const WIDTH1: usize = 2;
    const WIDTH2: usize = 3;
    enum JoinType {
        NestedLoop,
        HashEq,
    }

    pub fn scan1() -> TupleIterator {
        let tuples = create_tuple_list(vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]]);
        let ts = get_int_table_schema(WIDTH1);
        TupleIterator::new(tuples, ts)
    }

    pub fn scan2() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5],
            vec![4, 5, 6],
            vec![5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3],
            vec![3, 4, 3, 4, 5],
            vec![5, 6, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn gt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![3, 4, 1, 2, 3], // 1, 2 < 3
            vec![3, 4, 2, 3, 4],
            vec![5, 6, 1, 2, 3], // 1, 2, 3, 4 < 5
            vec![5, 6, 2, 3, 4],
            vec![5, 6, 3, 4, 5],
            vec![5, 6, 4, 5, 6],
            vec![7, 8, 1, 2, 3], // 1, 2, 3, 4, 5 < 7
            vec![7, 8, 2, 3, 4],
            vec![7, 8, 3, 4, 5],
            vec![7, 8, 4, 5, 6],
            vec![7, 8, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 2, 3, 4], // 1 < 2, 3, 4, 5
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6], 
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 4, 5, 6], // 3 < 4, 5
            vec![3, 4, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_or_eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3], // 1 <= 1, 2, 3, 4, 5
            vec![1, 2, 2, 3, 4], 
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6], 
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 3, 4, 5], // 3 <= 3, 4, 5
            vec![3, 4, 4, 5, 6], 
            vec![3, 4, 5, 6, 7],
            vec![5, 6, 5, 6, 7], // 5 <= 5
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    fn construct_join(
        ty: JoinType,
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
    ) -> Box<dyn OpIterator> {
        let s1 = Box::new(scan1());
        let s2 = Box::new(scan2());
        match ty {
            JoinType::NestedLoop => Box::new(Join::new(
                op,
                left_index,
                right_index,
                s1,
                s2,
            )),
            JoinType::HashEq => Box::new(HashEqJoin::new(
                op,
                left_index,
                right_index,
                s1,
                s2,
            )),
        }
    }

    fn test_get_schema(join_type: JoinType) {
        let op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let expected = get_int_table_schema(WIDTH1 + WIDTH2);
        let actual = op.get_schema();
        assert_eq!(&expected, actual);
    }

    fn test_next_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.next().unwrap();
    }

    fn test_close_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.close().unwrap();
    }

    fn test_rewind_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.rewind().unwrap();
    }

    fn test_rewind(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.open()?;
        while op.next()?.is_some() {}
        op.rewind()?;

        let mut eq_join = eq_join();
        eq_join.open()?;

        let acutal = op.next()?;
        let expected = eq_join.next()?;
        assert_eq!(acutal, expected);
        Ok(())
    }

    fn test_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let mut eq_join = eq_join();
        op.open()?;
        eq_join.open()?;
        match_all_tuples(op, Box::new(eq_join))
    }

    fn test_gt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::GreaterThan, 0, 0);
        let mut gt_join = gt_join();
        op.open()?;
        gt_join.open()?;
        match_all_tuples(op, Box::new(gt_join))
    }

    fn test_lt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThan, 0, 0);
        let mut lt_join = lt_join();
        op.open()?;
        lt_join.open()?;
        match_all_tuples(op, Box::new(lt_join))
    }

    fn test_lt_or_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThanOrEq, 0, 0);
        let mut lt_or_eq_join = lt_or_eq_join();
        op.open()?;
        lt_or_eq_join.open()?;
        match_all_tuples(op, Box::new(lt_or_eq_join))
    }

    mod join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn close_not_open() {
            test_close_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::NestedLoop);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::NestedLoop)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::NestedLoop)
        }

        #[test]
        fn gt_join() -> Result<(), CrustyError> {
            test_gt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_join() -> Result<(), CrustyError> {
            test_lt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_or_eq_join() -> Result<(), CrustyError> {
            test_lt_or_eq_join(JoinType::NestedLoop)
        }
    }

    mod hash_join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::HashEq);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::HashEq)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::HashEq)
        }
    }
}
