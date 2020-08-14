use rayon::{Scope, ThreadPool};

unsafe fn adjust_lifetime<'s, 'a, 'b>(scope: &'s Scope<'a>) -> &'s Scope<'b> {
    std::mem::transmute::<&'s Scope<'a>, &'s Scope<'b>>(scope)
}

/// Constructs scopes recursively.
/// Each recursion level takes (and removes) the first thread pool from
/// `pools`, creates a scope and pushes it into the `scopes` vector, then calls the next
/// recursion level inside this scope. Finally it calls `op` passing all the scopes to it.
fn nest<'scope, OP, R>(pools: &[&ThreadPool], scopes: Vec<&Scope<'scope>>, op: OP) -> R
where
    OP: for<'s> FnOnce(&'s [&'s Scope<'scope>]) -> R + 'scope + Send,
    R: Send,
{
    if !pools.is_empty() {
        pools[0].scope(move |s| {
            let mut scopes: Vec<&Scope<'scope>> = scopes;
            // Unfortunately Scope is invariant over lifetime, therefore we can't just
            // push all scopes into a single vector, because they have slightly different lifetimes
            // due to nesting, and they are different types to the compiler.
            // However, from the caller perspective, the recursion and nesting is invisible,
            // so we can force all scopes to having the same lifetime set to `scope by using this
            // unsafe trick. See https://github.com/rayon-rs/rayon/issues/782.
            scopes.push(unsafe { adjust_lifetime(s) });
            nest(&pools[1..], scopes, op)
        })
    } else {
        (op)(&scopes)
    }
}

/// Creates multiple Rayon scopes, one per given `ThreadPool`, around the given lambda `op`.
/// The purpose of this method is to be able to spawn tasks on multiple thread pools when
/// the number of thread pools is not known at compile-time. Same as with a single scope,
/// all tasks spawned by `op` are guaranteed to finish before this call exits, so they
/// are allowed to access structs from outside of the scope.
///
/// # Example
/// ```
/// use rayon::ThreadPoolBuilder;
/// use fclones::pools::multi_scope;
///
/// let pool1 = ThreadPoolBuilder::new().build().unwrap();
/// let pool2 = ThreadPoolBuilder::new().build().unwrap();
/// let common = vec![0, 1, 2]; // common data, must be Send
/// multi_scope(&[&pool1, &pool2], |scopes| {
///     scopes[0].spawn(|_| { /* execute on pool1, can use &common */ });
///     scopes[1].spawn(|_| { /* execute on pool2, can use &common */ });
/// });
/// ```
pub fn multi_scope<'scope, OP, R>(pools: &[&ThreadPool], op: OP) -> R
where
    OP: for<'s> FnOnce(&'s [&'s Scope<'scope>]) -> R + 'scope + Send,
    R: Send,
{
    nest(pools, Vec::with_capacity(pools.len()), op)
}

#[cfg(test)]
mod test {
    use super::*;
    use rayon::ThreadPoolBuilder;
    use std::collections::HashSet;
    use std::sync::mpsc::channel;
    use std::thread::ThreadId;

    fn distinct_thread_count(pool_id: i32, pairs: &Vec<(i32, ThreadId)>) -> usize {
        pairs
            .iter()
            .filter(|(p_id, _)| *p_id == pool_id)
            .map(|(_, thread_id)| thread_id)
            .collect::<HashSet<_>>()
            .len()
    }

    #[test]
    fn test_two_pools() {
        let pool0 = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let pool1 = ThreadPoolBuilder::new().num_threads(2).build().unwrap();

        let (tx, rx) = channel();
        multi_scope(&[&pool0, &pool1], move |scopes| {
            for _ in 0..1000 {
                let tx = tx.clone();
                scopes[0].spawn(move |_| tx.send((0, std::thread::current().id())).unwrap());
            }
            for _ in 0..1000 {
                let tx = tx.clone();
                scopes[1].spawn(move |_| tx.send((1, std::thread::current().id())).unwrap());
            }
        });

        let results: Vec<_> = rx.into_iter().collect();
        assert_eq!(distinct_thread_count(0, &results), 1);
        assert_eq!(distinct_thread_count(1, &results), 2);
    }
}
