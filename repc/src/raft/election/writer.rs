use super::Election;
struct Transaction<'a> {
    election: &'a mut Election,
}
