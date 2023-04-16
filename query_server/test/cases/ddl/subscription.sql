create subscription test on public destinations all "127.0.0.1:31001";

show subscription on public;

alter subscription test on public destinations all "127.0.0.1:31001" "127.0.0.1:31002";

show subscription on public;

drop subscription test on public;

show subscription on public;