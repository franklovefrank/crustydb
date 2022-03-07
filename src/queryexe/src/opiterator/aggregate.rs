use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::cmp::{max, min};
use std::collections::HashMap;

/// Contains the index of the field to aggregate and the operator to apply to the column of each group. (You can add any other fields that you think are neccessary)
#[derive(Clone)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}

/// Computes an aggregation function over multiple columns and grouped by multiple fields. (You can add any other fields that you think are neccessary)
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    ht: HashMap<Vec<Field>, Vec<Vec<Field>>>,
}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {
        Self {
            agg_fields: agg_fields,
            groupby_fields: groupby_fields,
            schema: schema.clone(),
            ht: HashMap::new(),
        }
    }


    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        let mut a_fields = Vec::new();
        let agg_iter = self.agg_fields.iter();
        for i in agg_iter {
            let item = tuple.get_field(i.field).unwrap().clone();
            a_fields.push(item);
        }

        let mut gb_fields = Vec::new();
        let gb_iter = self.groupby_fields.iter();
        for i in gb_iter {
            let item = tuple.get_field(*i).unwrap().clone();
            gb_fields.push(item);
        }
        
        let temp = Vec::new();

        self.ht
            .entry(gb_fields)
            .or_insert(temp)
            .push(a_fields);
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        let mut temp = Vec::new();
        for (gb_fields, a_tuples) in self.ht.iter() {
            let af = a_tuples.clone();
            let mut afs = Vec::new();

            for (i, agg) in self.agg_fields.iter().enumerate() {
                let mut col = Vec::new();
                for tuple in af.iter() {
                    col.push(tuple[i].clone());
                }
                match agg.op {
                    AggOp::Sum => {
                        let mut sum = 0;
                        for field in col {
                            let add = field.unwrap_int_field();
                            sum += add;
                        }
                        let res_f = Field::IntField(sum);
                        afs.push(res_f);
                    },
                    AggOp::Min => {
                        let iter = col.iter();
                        let min = iter.min();
                        let res = min.unwrap().clone();
                        afs.push(res);     
                    },
                    AggOp::Max => {
                        let iter = col.iter();
                        let max = iter.max();
                        let res = max.unwrap().clone();
                        afs.push(res);     
                    },
                    AggOp::Avg => {
                        let mut sum = 0;
                        let col_iter = col.iter();
                        for field in col_iter {
                            let add = field.unwrap_int_field();
                            sum += add;
                        }
                        let res = sum / col.len() as i32;
                        let f_res = Field::IntField(res);
                        afs.push(f_res);
                    }
                    AggOp::Count => {
                        let res = col.len() as i32;
                        let f_res = Field::IntField(res);
                        afs.push(f_res);
                    },
                }
            }
            let mut fs = gb_fields.clone();
            fs.extend(afs);
            let tuple = Tuple::new(fs);
            temp.push(tuple);
        }
        let schema_clone = self.schema.clone();
        let ret = TupleIterator::new(temp, schema_clone);
        return ret;
    }

}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
     /// Resulting schema.
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Tuple iterator
    t_iterator: TupleIterator,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        let mut data_types = Vec::new();
        let schema = child.get_schema();
        
        let op_iter = ops.iter();
        let agg_iter = agg_indices.iter();
        let zipped = op_iter.zip(agg_iter);
        let mut agg_fields = Vec::new();

        for (op, i) in zipped {
            let new = AggregateField{ op: op.clone(), field:i.clone()};
            agg_fields.push(new);
            match op {
                AggOp::Count => {
                let dt = DataType::Int;
                data_types.push(dt);
                }
                _ => {
                    let attr = schema.get_attribute(*i);
                    let a_type = attr.unwrap().dtype().clone();
                    data_types.push(a_type);
                }
            }
        }

        let gb_iter = groupby_indices.iter();

        for i in gb_iter {
            let attribute = schema.get_attribute(*i);
            let res = attribute.unwrap().dtype().clone();
            data_types.push(res);
        }

        let mut gbnames = groupby_names.clone();
        let agg_clone = agg_names.clone();
        gbnames.extend(agg_clone);
        let res_schema = TableSchema::from_vecs(gbnames, data_types);

        let aggregator = Aggregator::new(agg_fields, groupby_indices, &res_schema);
        let agg_iterator = aggregator.iterator();

        let mut new = Aggregate {
            schema: res_schema,
            open: false,
            t_iterator: agg_iterator
        };
        let res  = new.helper(aggregator, child);
        match res {
            Ok(()) => new,
            Err(e)=> panic!("something went wrong")
        }
    }

    fn helper(
        &mut self,
        mut agg: Aggregator,
        mut child: Box<dyn OpIterator>,
    ) -> Result<(), CrustyError> {
        let open = child.open();
        match open {
            Err(e) => return Err(CrustyError::CrustyError(String::from("error in helper"))),
            Ok(()) => {
                while let Some(t) = child.next()? {
                    agg.merge_tuple_into_group(&t);
                }
                self.t_iterator = agg.iterator();
                let res = child.close();
                match res {
                    Ok(()) => Ok(()),
                    Err(e)=> Err(CrustyError::CrustyError(String::from("error in helper")))
                }
            }
        }
    }

}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        return self.t_iterator.open();
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if self.open {
            return self.t_iterator.next();
        }
        else {
            return Err(CrustyError::CrustyError(String::from("op hasn't been opened yet")))
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if self.open == false{
            panic!("it's not open anyway");
        }
        self.open = false;
        let res = self.t_iterator.close();
        match res {
            Ok(()) => return Ok(()),
            Err(e) => return Err(CrustyError::CrustyError(String::from("couldn't close"))),
        }
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError(String::from("op hasn't been opened yet")));
        }
        let rewind = self.t_iterator.rewind();
        match rewind {
            Ok(()) => {
                let close = self.close();
                match close {
                    Ok(()) => return self.open(),
                    Err(e) => return Err(CrustyError::CrustyError(String::from("couldn't close")))
                }
            }
            Err(e) => return Err(CrustyError::CrustyError(String::from("couldn't rewind")))
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

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(
                vec![AggregateField { field, op }],
                Vec::new(),
                &schema,
            );
            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while iter.next()?.is_some() {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            ai.open()?;
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?;
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;
            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![Field::IntField(1), Field::IntField(3), Field::IntField(2), Field::IntField(2)],
                vec![Field::IntField(1), Field::IntField(4), Field::IntField(1), Field::IntField(3)],
                vec![Field::IntField(2), Field::IntField(4), Field::IntField(1), Field::IntField(4)],
                vec![Field::IntField(2), Field::IntField(5), Field::IntField(2), Field::IntField(6)],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "avg", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
