% the function receives in input a data file a window size in seconds and
% the nubmer of transaction types
% and returns 2 matrixes containing the number of transactions per type per
% window and the average latency per window



%filename = 'coefs-7200-20-20-20-20-3000';
%winSize=1;
%numVariable=5;
function [counts  latencies  monitor] = fast_deverticalize_align(indir, outdir, proxyfilename, monitorfilename, winSize, numVariable)
    
    tic;
    % load the file skipping the first 4 lines
    temp = csvread(horzcat(indir,proxyfilename),4);
    monitor = csvread(horzcat(indir,monitorfilename),2);
    
    %bring measure in timewindows (if winSize is 1 than it is in seconds)
    temp(:,2) = temp(:,2)/(winSize);
    temp(:,3) = temp(:,3)/(winSize*1000000);
    
    fprintf(1, 'READ FROM DISK TIME:');
    toc;
    
    tic;
    
    
    
    counts = zeros(size(monitor,1),numVariable+1);
    latencies = zeros(size(monitor,1),numVariable+1);
    latenciesPCtile = zeros(size(monitor,1),numVariable+1,8);
%    locktimes = zeros(size(monitor,1),numVariable+1);
    
    
    for i=1:size(counts,1)-1
        for j=1:numVariable
            counts(i,j+1) = sum(temp(:,1)==j & temp(:,2)+temp(:,3)>=monitor(i,1) & temp(:,2)+temp(:,3)<monitor(i+1,1));
            latencies(i,j+1) = sum(sum(temp(find(temp(:,1)==j & temp(:,2)+temp(:,3)>=monitor(i,1) & temp(:,2)+temp(:,3)<monitor(i+1,1)),3)));
 %           locktimes(i,j+1) = sum(sum(temp(find(temp(:,1)==j & temp(:,2)+temp(:,3)>=monitor(i,1) & temp(:,2)+temp(:,3)<monitor(i+1,1)),4)));
            latenciesPCtile(i,j+1,:) = prctile(temp(find(temp(:,1)==j & temp(:,2)+temp(:,3)>=monitor(i,1) & temp(:,2)+temp(:,3)<monitor(i+1,1)),3),[10, 25, 50, 75, 90, 95, 99, 99.9]);    
        end
        
    end
    
    latencies = latencies./counts; % actually compute latency AVG
  %  locktimes = locktimes./counts;
    counts(:,1) = monitor(:,1);
    latencies(:,1) = counts(:,1); % reset first cell
   % locktimes(:,1) = counts(:,1);
    latencies(isnan(latencies))=0;                                  % remove NaN  
    %locktimes(isnan(locktimes))=0;                                  % remove NaN  
    latenciesPCtile(:,1) = counts(:,1);
    
    
    fprintf(1, 'CRUNCH TIME:');
    toc;
    
    fprintf(1, 'SAVE TIME:');
    tic;
    save(horzcat(outdir,proxyfilename,'_rough_trans_count.al'), 'counts','-ascii','-double');
    save(horzcat(outdir,proxyfilename,'_avg_latency.al'), 'latencies','-ascii','-double');
    %save(horzcat(outdir,proxyfilename,'_locktimes.al'), 'locktimes','-ascii','-double');
    save(horzcat(outdir,proxyfilename,'_prctile_latencies.mat'), 'latenciesPCtile');
    toc;
    
end
