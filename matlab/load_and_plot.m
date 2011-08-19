
function load_and_plot(monitorfile, transactionprefix)

monitor = csvread(monitorfile,2);
avglat = load(horzcat(transactionprefix,'_avg_latency.al'));
prclat = load(horzcat(transactionprefix,'_prctile_latencies.mat'));
%locktime = load(horzcat(transactionprefix,'percona_transactions.csv_locktimes.al'));
counts = load(horzcat(transactionprefix,'percona_transactions.csv_rough_trans_count.al'));

Com_commit_index = 164;
disk_write_index = 108;
cpu_indexes =       [5    11    17    23    29    35    41    47    53    59    65    71    77    83    89    95     6    12    18    24    30    36    42    48    54    60    66    72    78    84    90    96];

tstart=2;
tend=size(monitor,1);

figure;
subplot(3,2,1);
plot(monitor(tstart:tend,cpu_indexes));
title('cpu usage');
xlabel('time');
ylabel('cpu usage (% of core)');
grid on;
%axis([0 500 0 100]);


subplot(3,2,3);
plot(monitor(tstart:tend,disk_write_index)./1024./1024);
title('disk usage');
xlabel('time');
ylabel('disk usage (MB/sec)');
grid on;


subplot(3,2,5);
plot(diff(monitor(tstart:tend,Com_commit_index)));
hold on;
plot(sum(counts(tstart:tend,2:end)'),'r');
grid;
title('transactions');
xlabel('time (sec)');
ylabel('transactions (tps)');
legend('Comcommit','perconaLog');
grid on;



subplot(3,2,2);
plot(sum(avglat(tstart:tend,2:end)'));
hold on;
plot(sum(prclat.latenciesPCtile(tstart:tend,2:end,6)'),'r'); % showing 95%tile
title('latency');
xlabel('time');
ylabel('latency (sec)');
grid on;

%subplot(3,2,4);
%plot(locktime(tstart:tend,2:end));
%title('locktime');
%xlabel('time');
%ylabel('lock time (sec)');
%grid on;


set(gcf,'Color','w');

end