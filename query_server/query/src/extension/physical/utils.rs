use std::fmt;

use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan};

pub fn one_line(plan: &dyn ExecutionPlan) -> impl fmt::Display + '_ {
    struct Wrapper<'a> {
        plan: &'a dyn ExecutionPlan,
    }
    impl<'a> fmt::Display for Wrapper<'a> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let t = DisplayFormatType::Default;
            self.plan.fmt_as(t, f)
        }
    }

    Wrapper { plan }
}
