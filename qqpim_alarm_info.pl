#!/usr/bin/perl
# Author :ceaglecao
# Script: sec_alarm_info.pl 
# Version: 1.0
# Desc :从监控系统拉取应用中心关键监控项的告警进行监控统计，超过阈值入库并有监控邮件提醒录单
#
########### 脚本配置项 ############
# stat提取语句：line59~line64
# 监控stat入库表：line123
# 监控info入库表：line245
# 监控ecstatus表更新$stat_name：line188, line246


use strict;
use warnings;
use POSIX qw( strftime ceil floor );
use DBI;

##################################################################################################################
#################################                        Define variables here        ########################################
##################################################################################################################
my %hash;
my $stat_date = `date -d "now " +%F`;chomp($stat_date); #统计日期
my $stat_tm = `date -d "now" +"%H:%M"`; chomp $stat_tm;
if ($stat_tm =~ /(\d+):(\d+)/) {
    $stat_tm = sprintf("%02d%02d", $1,  ceil(($2/10.0) + 0.05) * 10);
}
my $alarm_date = `date -d "1 day ago " +%F`;chomp($alarm_date);  #告警日期
##################################################################################################################
#################################                        Function declaration         ########################################
##################################################################################################################
sub init;                                                               #初始化
sub get_statistic_data;                                 #获取告警信息
sub statistic_data_to_db;                               #展示所有统计好的数据,统计数据入库
sub     format_time_to_standard;                                                        #统计时间转换为标准时间
sub format_time_to_stat;                                                                #标准时间转化为统计时间
sub alarm_info_to_db;
sub alarm_info;                                         #获取需要录单的告警

##################################################################################################################
#################################            Define functions here        ########################################
##################################################################################################################
sub init
{
        system("mkdir -p /data/taf/taf_alarm/bin");
        system("mkdir -p /data/taf/taf_alarm/logs");
}
sub get_statistic_data   # 获取告警信息
{
        my $mysql = "10.166.133.138";
        my $port ="3300";
        my $user = "moniuser";
        my $passwd ="moniuser2009";
        my $db = "db_alarm";
        my $dbh = DBI->connect("dbi:mysql:$db:$mysql:$port", $user, $passwd) or die "connect db $mysql failed.\n";
        $dbh->do("SET character_set_client = 'gbk' ");
        $dbh->do("SET character_set_connection = 'gbk'");
        $dbh->do("SET character_set_results= 'gbk' ");
        #mysql -h 10.166.133.138 -P3300 -umoniuser -pmoniuser2009 --default-character-set=gbk -D db_alarm desc t_info_alarm

        my $sql_data;
        my $stat_tm = `date -d "now" +"%H:%M"`; chomp $stat_tm;
        if ($stat_tm =~ /(\d+):(\d+)/) {
        $stat_tm = sprintf("%02d%02d", $1,  ceil(($2/10.0) + 0.05) * 10);
        }

        #$sql_data  = qq { select b.appgroup,b.monitor_type,a.appname,a.f_date,a.f_tflag,a.subappname,a.subappvalue,a.xyzvalue,a.redinfo      
        #from t_info_alarm a,(select appname,appgroup,monitor_type from t_cfg_app where appgroup='LOGIN_ECC' )as b  
        #where   a.appname=b.appname  and a.f_date="$alarm_date" };
        $sql_data  = qq { select b.appgroup,b.monitor_type,a.appname,a.f_date,a.f_tflag,a.subappname,a.subappvalue,a.xyzvalue,a.redinfo}.      
        qq { from t_info_alarm a,(select appname,appgroup,monitor_type from t_cfg_app where }.
        qq { appname='ts_qqpim')as b}.
        qq { where   a.appname=b.appname  and a.f_date="$alarm_date" }.
        qq { and (a.subappvalue like '\%SecureServer\%' or a.subappvalue like '\%UpdateTipsServer\%' or a.subappvalue like '\%MsgTipsServer\%'}.
        qq { or a.subappvalue like '\%SecureSoftInfoServer\%' or a.subappvalue like '\%CloudCheckServer\%')}; #通过appgroup"监控组"或appname"监控项"指定需要提取的告警
        printf("%s\n",$sql_data);


        #a.appname='ts_kqq' and
        my $sth_data = $dbh->prepare($sql_data);
        $sth_data->execute();
        my $line = $sth_data->rows();
        while (my $ref = $sth_data->fetchrow_hashref()) {
            my $alarm_date = $ref->{'f_date'};                                  #告警日期
            my $alarm_tflag = $ref->{'f_tflag'};                                #告警时间
            my $monitor_group = $ref->{'appgroup'};                             #监控组
            my $monitor_name = $ref->{'appname'};                               #监控项
            my $monitor = "$monitor_group"."."."$monitor_name"." ".$ref->{'subappvalue'};
            my $monitor_type = $ref->{'monitor_type'};                          #监控类型
            my $dimension_values = $ref->{'xyzvalue'};                          #维度内容
            my $alarm_all = $ref->{'redinfo'};                      #告警内容
            #my $alarm_all = "超时率:99.72%#总流量:12697,同比波动-98.67%";
            my @alarm_index = split(/\#/,$alarm_all);

            #获取收敛规则
            foreach my $alarm_info (@alarm_index) {
                        $alarm_info =~ s/:.*$//g;
               my $sql_data1  = qq { select * from t_cfg_property where appname="$monitor_name" and property="$alarm_info" };
               my $sth_data1 = $dbh->prepare($sql_data1);
               $sth_data1->execute();  
               my $l= $sth_data1->rows();

               while (my $ref1 = $sth_data1->fetchrow_hashref()) {
                   my $threshold    = $ref1->{'threshold'};     #告警收敛信息
                   my $appname = $ref1->{'appname'};
                   my $property = $ref1->{'property'};
                   if ( $threshold =~/exp2$/) {
                           $threshold = "exp2";       #阀值二次收敛
                   } else {
                           $threshold = "normal";          #未收敛
                   }
                   my $dims="$alarm_date"."|"."$alarm_tflag"."|"."$monitor"."|"."$monitor_type"."|"."$threshold"."|"."$dimension_values"."|"."$alarm_info";
                   $hash{$dims} = 1;
                   printf("%-16s%-8s%-48s%-8s%-8s%-32s%-32s\n",$alarm_date,$alarm_tflag,$monitor,$monitor_type,$threshold,$dimension_values,$alarm_info);

               }
            }
        }
        $sth_data -> finish();
        $dbh->disconnect();
}

sub statistic_data_to_db
{
        ####################################################################################################################
        # 入库
        my $mysql1  = "10.166.133.138";
        my $user1   = "taf";
        my $passwd1 = "taf2011";
        my $dbh1 = DBI->connect("dbi:mysql:db_tafping:$mysql1:3301", $user1, $passwd1) or die "connect db $mysql1 failed.\n";
        $dbh1->do("SET character_set_client = 'gbk' ");
        #mysql --default-character=gbk -utaf -ptaf2011 -h10.166.133.138 -P3301 db_tafping

        my $table_prefix  = `date -d "1 day ago" +"%Y%m%d"` ; chomp $table_prefix;
        my $table_name = "t_imdata_qqpim_alarm_stat"."_". $table_prefix; #告警数据入库表名,按天分表,alarm_stat
        my $stat_tm = `date -d "now" +"%H:%M"`; chomp $stat_tm;

        if ($stat_tm =~ /(\d+):(\d+)/) {
                        $stat_tm = sprintf("%02d%02d", $1,  ceil(($2/10.0) + 0.05) * 10);
        }
        my $create_table_sql = qq{
        create table if not exists  $table_name  (
               `f_date` date NOT NULL default '1970-01-01',
               `f_tflag` varchar(8) NOT NULL default '',
               `f_monitor` varchar(64) NOT NULL default '',
               `f_monitor_type` varchar(64) NOT NULL default '',
               `f_threshold` varchar(32) NOT NULL default '',
               `f_dimension_values` varchar(64) NOT NULL default '',
               `f_alarm_info` varchar(128) NOT NULL default '0',
               `f_alarm_num` bigint(20) NOT NULL default '0',
               PRIMARY KEY  (`f_date`,`f_tflag`,`f_monitor`,`f_monitor_type`,`f_threshold`,`f_dimension_values`,`f_alarm_info`)
        ) ENGINE=MyISAM DEFAULT CHARSET=gbk
        };
        $dbh1->do($create_table_sql);
####################################################################################################
        # my $sql_data1_total  = qq { select f_appserver,f_node_name,f_reason,f_reason_num 
                #                                                               from $table_name
                #                                                       where f_tflag<'$tm';  };
        # my $sth_data1_total = $dbh1->prepare($sql_data1_total);
                # $sth_data1_total->execute();
                # while (my $ref = $sth_data1_total->fetchrow_hashref()) {
                #          my $server = $ref->{'f_appserver'};
                #          my $node_name = $ref->{'f_node_name'};
                #          my $reason = $ref->{'f_reason'};
                #          my $reason_num = $ref->{'f_reason_num'};
        #       $hash{$server}{$node_name}{$reason}{"total"} += $reason_num;

                # }

#####################################################################################################
        my $sql_data1  = qq { INSERT INTO $table_name 
                             (f_date, f_tflag, f_monitor,f_monitor_type,f_threshold,f_dimension_values,f_alarm_info,f_alarm_num)  
                             VALUES (?,?,?,?,?,?,?,?) };
        my $sth_data1 = $dbh1->prepare($sql_data1);
                foreach my $dim (keys %hash) {
                        my ($alarm_date,$alarm_tflag,$monitor,$monitor_type,$threshold,$dimension_values,$alarm_info) = split(/\|/,$dim);

                        my $alarm_num = $hash{$dim};
                                                printf("%-16s%-8s%-48s%-8s%-8s%-32s%-32s%-8d\n",$alarm_date,$alarm_tflag,$monitor,$monitor_type,$threshold,$dimension_values,$alarm_info,$alarm_num);

                        $sth_data1->execute($alarm_date,$alarm_tflag,$monitor,$monitor_type,$threshold,$dimension_values,$alarm_info,$alarm_num);
                        #printf("%-16s%-8s%-48s%-8s%-8s%-32s%-32s%-8d\n",$alarm_date,$alarm_tflag,$monitor,$monitor_type,$threshold,$dimension_values,$alarm_info,$alarm_num);
                }

}

sub alarm_info
{

    my $mysql1  = "10.166.133.138";
    my $user1   = "taf";
    my $passwd1 = "taf2011";
    my $dbh1 = DBI->connect("dbi:mysql:db_tafping:$mysql1:3301", $user1, $passwd1) or die "connect db $mysql1 failed.\n";
    $dbh1->do("SET character_set_client = 'gbk' ");
    $dbh1->do("SET character_set_results= 'gbk' "); 
    #mysql --default-character=gbk -utaf -ptaf2011 -h10.166.133.138 -P3301 db_tafping
    my $sql_data1;
    my $table_prefix  = `date -d "1 day ago" +"%Y%m%d"` ; chomp $table_prefix;
    my $table_name = "t_imdata_qqpim_alarm_stat"."_". $table_prefix;
    my $stat_name = "qqpim_alarm_stat";

        $sql_data1  = qq { select  f_date, f_tflag, f_monitor,f_monitor_type,f_threshold,f_dimension_values,f_alarm_info,f_alarm_num 
                       from $table_name where f_date="$alarm_date"  };    
    my %alarm;
    my $sth_data1 = $dbh1->prepare($sql_data1); 
    $sth_data1->execute();
    while (my $ref = $sth_data1->fetchrow_hashref()) {  
        my $alarm_date = $ref->{'f_date'};
        my $alarm_tflag = $ref->{'f_tflag'};
        my $tm = $alarm_date." ".$alarm_tflag;   
        my $monitor_name = $ref->{'f_monitor'};    
        my $monitor_type = $ref->{'f_monitor_type'};
        my $threshold = $ref->{'f_threshold'};
        my $sub_monitor_value = $ref->{'f_sub_monitor_value'};                             
        my $dimension_values = $ref->{'f_dimension_values'};  
        my $alarm_info = $ref->{'f_alarm_info'}; 
        my $alarm_num = $ref->{'f_alarm_num'};

        my $sql_data2  = qq { select * from $table_name where f_monitor='$monitor_name' and f_monitor_type='$monitor_type' and f_threshold='$threshold' 
                and f_alarm_info='$alarm_info' and f_date='$alarm_date'};    
        my $sth_data2 = $dbh1->prepare($sql_data2); 
        $sth_data2->execute();
        my $alarm_times = $sth_data2->rows();

        my $dim = "$stat_date"."|"."$stat_tm"."|"."$alarm_date"."|"."$monitor_name"."|"."$monitor_type"."|"."$threshold"."|"."$alarm_info";
        if ($threshold eq "exp2") {
                if ($alarm_times >=2 ) {
                        $alarm{$dim}{"alarm_times"} = $alarm_times;
                }
        } else {
                if ($alarm_times >=5 ) {
                        $alarm{$dim}{"alarm_times"} = $alarm_times;
                }
        }
     }  
     #监控信息入库
     foreach my $dim (keys %alarm) { 
              my ($stat_date,$stat_tm,$alarm_date,$monitor_name,$monitor_type,$threshold,$alarm_info) = split(/\|/,$dim);
              my $alarm_times = $alarm{$dim}{"alarm_times"};
              printf("连续1小时告警>=3次:%-8s%-8s%-16s%-16s%-16s%-16s%-16s%-8d\n",$stat_date,$stat_tm,$alarm_date,$monitor_name,$monitor_type,$threshold,$alarm_info,$alarm_times);
              alarm_info_to_db($stat_date,$stat_tm,$alarm_date,$monitor_name,$monitor_type,$threshold,$alarm_info,$alarm_times);
     }

}
sub alarm_info_to_db
{
        ####################################################################################################################
        # 入库
        my $mysql1  = "10.166.133.138";
        my $user1   = "taf";
        my $passwd1 = "taf2011";
        my $dbh1 = DBI->connect("dbi:mysql:db_tafping:$mysql1:3301", $user1, $passwd1) or die "connect db $mysql1 failed.\n";
        $dbh1->do("SET character_set_client = 'gbk' ");
        #mysql --default-character=gbk -utaf -ptaf2011 -h10.166.133.138 -P3301 db_tafping

        my $table_prefix  = `date -d "now" +"%Y%m%d"` ; chomp $table_prefix;
        my $table_name = "t_imdata_qqpim_alarm_info"."_". $table_prefix; #监控信息入库表名,按天分表,alarm_info
        my $stat_name = "qqpim_alarm_info";

        my $create_table_sql = qq{
        create table if not exists  $table_name  (
               `f_date` date NOT NULL default '1970-01-01',
               `f_tflag` varchar(8) NOT NULL default '',
               `f_alarm_date` varchar(16) NOT NULL default '',
               `f_mon` varchar(64) NOT NULL default '',
               `f_mon_type` varchar(64) NOT NULL default '',
               `f_threshold` varchar(32) NOT NULL default '',
               `f_alarm_info` varchar(128) NOT NULL default '0',
               `f_alarm_times` bigint(20) NOT NULL default '0',
               PRIMARY KEY  (`f_date`,`f_tflag`,`f_alarm_date`,`f_mon`,`f_mon_type`,`f_threshold`,`f_alarm_info`)
        ) ENGINE=MyISAM DEFAULT CHARSET=gbk
        };
        $dbh1->do($create_table_sql);

        my ($stat_date,$stat_tm,$alarm_date,$monitor_name,$monitor_type,$threshold,$alarm_info,$alarm_times) = @_;


            
        printf("%s%-8s%-8s%-16s%-16s%-16s%-8s%-16s%-8d\n",$table_name,$stat_date,$stat_tm,$alarm_date,$monitor_name,$monitor_type,$threshold,$alarm_info,$alarm_times);
        my $sql_data3  = qq { INSERT INTO $table_name 
                             (f_date, f_tflag, f_alarm_date,f_mon,f_mon_type,f_threshold,f_alarm_info,f_alarm_times)  
                             VALUES (?,?,?,?,?,?,?,?) };
        my $sth_data3 = $dbh1->prepare($sql_data3);
        $sth_data3->execute($stat_date,$stat_tm,$alarm_date,$monitor_name,$monitor_type,$threshold,$alarm_info,$alarm_times);

        my $sql_ecstatus1 = qq { replace into t_ecstatus (appname,checkint,lasttime) values(?, ?, ?) };
        my $sth_ecstatus1 = $dbh1->prepare($sql_ecstatus1);

        $stat_date =~ s/\-//g;
        $sth_ecstatus1->execute("$stat_name", 10, "$stat_date $stat_tm" );


}


init;
get_statistic_data;
statistic_data_to_db;
alarm_info;