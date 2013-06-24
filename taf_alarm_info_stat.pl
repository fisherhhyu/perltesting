#!/usr/bin/perl
# Author :ceaglecao
# Script: taf_alarm_info_stat.pl
# Version: 1.0
# Desc :从监控系统拉取所有taf的告警和cdb告警数据进行一下两种统计，并有邮件提醒录单
use strict;
use warnings;
use POSIX qw( strftime ceil floor );
use DBI;

##################################################################################################################
#################################                        Define variables here        ########################################
##################################################################################################################
my %hash;
my $stat_date = `date -d "now " +%F`;chomp($stat_date);
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
                if ( $stat_tm =~/^00/) {
                        $stat_date = `date -d "1 day ago " +%F`;chomp($stat_date);
                        $sql_data  = qq { select b.appgroup,b.monitor_type,a.appname,a.f_date,a.f_tflag,a.subappname,a.subappvalue,a.xyzvalue,a.redinfo      
                        from t_info_alarm a,(select appname,appgroup,monitor_type from t_cfg_app where appgroup='TAF_STAT' or appgroup='TAF_Property')as b  
                        where   a.appname=b.appname  and a.f_date="$stat_date" and  a.f_tflag >='1710' and  a.f_tflag <='2360'};

                } elsif ( $stat_tm =~/^09/ ) {
                        $sql_data  = qq { select b.appgroup,b.monitor_type,a.appname,a.f_date,a.f_tflag,a.subappname,a.subappvalue,a.xyzvalue,a.redinfo      
                        from t_info_alarm a,(select appname,appgroup,monitor_type from t_cfg_app where appgroup='TAF_STAT' or appgroup='TAF_Property')as b  
                        where   a.appname=b.appname  and a.f_date="$stat_date" and  a.f_tflag >='0010' and  a.f_tflag <='0860'};
                } elsif ($stat_tm =~/^17/ ) {
                        $sql_data  = qq { select b.appgroup,b.monitor_type,a.appname,a.f_date,a.f_tflag,a.subappname,a.subappvalue,a.xyzvalue,a.redinfo      
                        from t_info_alarm a,(select appname,appgroup,monitor_type from t_cfg_app where appgroup='TAF_STAT' or appgroup='TAF_Property')as b  
                        where   a.appname=b.appname  and a.f_date="$stat_date" and  a.f_tflag >='0910' and  a.f_tflag <='1660'};
                }


        #a.appname='ts_kqq' and
        my $sth_data = $dbh->prepare($sql_data);
        $sth_data->execute();
        my $line = $sth_data->rows();
        while (my $ref = $sth_data->fetchrow_hashref()) {
            my $alarm_date = $ref->{'f_date'};									#告警时间

            my $alarm_tflag = $ref->{'f_tflag'};                                #告警时间
            my $monitor_group = $ref->{'appgroup'};                             #监控组
            my $monitor_name = $ref->{'appname'};                               #监控项
            my $monitor = "$monitor_group"."."."$monitor_name";
            my $monitor_type = $ref->{'monitor_type'};                          #监控类型
            my $sub_monitor_name = $ref->{'subappname'};                        #子监控名
            my $sub_monitor_value = $ref->{'subappvalue'};                      #子监控区分字段，可以通过该字段获取服务模块信息
            if ( $sub_monitor_value eq "") {$sub_monitor_value = "$monitor_name" };
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
                   $hash{$alarm_date}{$alarm_tflag}{$monitor}{$monitor_type}{$threshold}{$sub_monitor_value}{$dimension_values}{$alarm_info} = 1;
                   printf("%-16s%-8s%-48s%-8s%-8s%-32s%-32s%-32s\n",$alarm_date,$alarm_tflag,$monitor,$monitor_type,$threshold,$sub_monitor_value,$dimension_values,$alarm_info);

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

        my $table_prefix  = `date -d "now" +"%Y%m%d"` ; chomp $table_prefix;
        my $table_name = "t_imdata_taf_alarm_stat"."_". $table_prefix;
        my $stat_name = "taf_alarm_stat";
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
               `f_sub_monitor_value`  varchar(64) NOT NULL default '',
               `f_dimension_values` varchar(64) NOT NULL default '',
               `f_alarm_info` varchar(128) NOT NULL default '0',
               `f_alarm_num` bigint(20) NOT NULL default '0',
               PRIMARY KEY  (`f_date`,`f_tflag`,`f_monitor`,`f_monitor_type`,`f_threshold`,`f_sub_monitor_value`,`f_dimension_values`,`f_alarm_info`)
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
                             (f_date, f_tflag, f_monitor,f_monitor_type,f_threshold,f_sub_monitor_value,f_dimension_values,f_alarm_info,f_alarm_num)  
                             VALUES (?,?,?,?,?,?,?,?,?) };
        my $sth_data1 = $dbh1->prepare($sql_data1);

        foreach my $alarm_date ( keys %hash) {
                        foreach my $alarm_tflag ( keys %{$hash{$alarm_date}}) {
                                foreach my $monitor ( keys %{$hash{$alarm_date}{$alarm_tflag}}) {
                                        foreach my $monitor_type ( keys %{$hash{$alarm_date}{$alarm_tflag}{$monitor}}) {
                                                foreach my $threshold ( keys %{$hash{$alarm_date}{$alarm_tflag}{$monitor}{$monitor_type}}) {
                                                        foreach my $sub_monitor_value (keys %{$hash{$alarm_date}{$alarm_tflag}{$monitor}{$monitor_type}{$threshold}}) {
                                                                foreach my $dimension_values (keys %{$hash{$alarm_date}{$alarm_tflag}{$monitor}{$monitor_type}{$threshold}{$sub_monitor_value}}) {
                                                                        foreach my $alarm_info (keys %{$hash{$alarm_date}{$alarm_tflag}{$monitor}{$monitor_type}{$threshold}{$sub_monitor_value}{$dimension_values}}) {
                                                                                my $alarm_num = $hash{$alarm_date}{$alarm_tflag}{$monitor}{$monitor_type}{$threshold}{$sub_monitor_value}{$dimension_values}{$alarm_info};
                                                                                $sth_data1->execute($alarm_date,$alarm_tflag,$monitor,$monitor_type,$threshold,$sub_monitor_value,$dimension_values,$alarm_info,$alarm_num);
                                                                                printf("%-16s%-8s%-48s%-8s%-8s%-32s%-32s%-32s%-8d\n",$alarm_date,$alarm_tflag,$monitor,$monitor_type,$threshold,$sub_monitor_value,$dimension_values,$alarm_info,$alarm_num);
                                    }
                                }
                            }
                        }
                    }
                }
            }
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
    my $table_prefix  = `date -d "now" +"%Y%m%d"` ; chomp $table_prefix;
    my $table_name = "t_imdata_taf_alarm_stat"."_". $table_prefix;
    my $stat_name = "taf_alarm_stat";
        my $stat_tm = `date -d "now" +"%H:%M"`; chomp $stat_tm;
        if ($stat_tm =~ /(\d+):(\d+)/) {
            $stat_tm = sprintf("%02d%02d", $1,  ceil(($2/10.0) + 0.05) * 10);
        }
        if ( $stat_tm =~/^00/) {
				$stat_date = `date -d "1 day ago" +"%F"`;chomp($stat_date);
                $sql_data1  = qq { select  f_date, f_tflag, f_monitor,f_monitor_type,f_threshold,f_sub_monitor_value,f_dimension_values,f_alarm_info,f_alarm_num 
                           from $table_name where f_date="$stat_date" and  f_tflag >='1710' and  f_tflag <='2360' };    
        } elsif ( $stat_tm =~/^09/ ) {
                $sql_data1  = qq { select  f_date, f_tflag, f_monitor,f_monitor_type,f_threshold,f_sub_monitor_value,f_dimension_values,f_alarm_info,f_alarm_num 
                           from $table_name where f_date="$stat_date" and  f_tflag >='0010' and  f_tflag <='0860' };    

        } elsif ($stat_tm =~/^17/ ) {
                $sql_data1  = qq { select  f_date, f_tflag, f_monitor,f_monitor_type,f_threshold,f_sub_monitor_value,f_dimension_values,f_alarm_info,f_alarm_num 
                       from $table_name where f_date="$stat_date" and  f_tflag >='0910' and  f_tflag <='1660' };    

        }
    my %alarm;
    my $sth_data1 = $dbh1->prepare($sql_data1); 
    $sth_data1->execute();
    while (my $ref = $sth_data1->fetchrow_hashref()) {  
        my $date = $ref->{'f_date'};
        my $tflag = $ref->{'f_tflag'};
        my $tm = $date." ".$tflag;   
        my $monitor_name = $ref->{'f_monitor'};    
        my $monitor_type = $ref->{'f_monitor_type'};
        my $threshold = $ref->{'f_threshold'};
        my $sub_monitor_value = $ref->{'f_sub_monitor_value'};                             
        my $dimension_values = $ref->{'f_dimension_values'};  
        my $alarm_info = $ref->{'f_alarm_info'}; 
        my $alarm_num = $ref->{'f_alarm_num'};
        my $end_time_tmp = format_time_to_standard($tm);    #将出现告警的时间转化为标准时间
        my ($date_end,$tm_end) = split(/\s+/,format_time_to_stat($end_time_tmp));#计算1小时后的统计时间

        my $sql_data2  = qq { select * from $table_name where f_monitor='$monitor_name' and f_monitor_type='$monitor_type' and f_threshold='$threshold' 
                and f_sub_monitor_value='$sub_monitor_value' and f_alarm_info='$alarm_info' and f_date='$date' and f_tflag>='$tflag' and f_tflag<='$tm_end' };    
        my $sth_data2 = $dbh1->prepare($sql_data2); 
        $sth_data2->execute();
		my $alarm_times = $sth_data2->rows();
        my $alarm_interval = "$tflag"."~~"."$tm_end";
        $tflag=$stat_tm;
        if ($threshold eq "exp2") {
                if ($alarm_times >=2 ) {
                        $alarm{$date}{$tflag}{$alarm_interval}{$monitor_name}{$monitor_type}{$threshold}{$sub_monitor_value}{$alarm_info}{"alarm_times"} = $alarm_times;
                }
        } else {
                if ($alarm_times >=5 ) {
                        $alarm{$date}{$tflag}{$alarm_interval}{$monitor_name}{$monitor_type}{$threshold}{$sub_monitor_value}{$alarm_info}{"alarm_times"} = $alarm_times;
                }
        }
     }  
     #监控信息入库
     foreach my $date (keys %alarm ) {
             foreach my $tflag (keys %{$alarm{$date}}) {
				foreach my $alarm_interval (keys %{$alarm{$date}{$tflag}}) {
                             foreach my $monitor_name (keys %{$alarm{$date}{$tflag}{$alarm_interval}}) {
                                     foreach my $monitor_type (keys %{$alarm{$date}{$tflag}{$alarm_interval}{$monitor_name}}) {
                                             foreach my $threshold (keys %{$alarm{$date}{$tflag}{$alarm_interval}{$monitor_name}{$monitor_type}}) { 
                                                     foreach my $sub_monitor_value (keys %{$alarm{$date}{$tflag}{$alarm_interval}{$monitor_name}{$monitor_type}{$threshold}}) {
                                                             foreach my $alarm_info (keys %{$alarm{$date}{$tflag}{$alarm_interval}{$monitor_name}{$monitor_type}{$threshold}{$sub_monitor_value}}) { 
                                                                     my $alarm_times = $alarm{$date}{$tflag}{$alarm_interval}{$monitor_name}{$monitor_type}{$threshold}{$sub_monitor_value}{$alarm_info}{"alarm_times"};
                                                                     printf("连续1小时告警>=3次:%-8s%-8s%-16s%-16s%-16s%-8s%-16s%-16s%-8d\n",$date,$tflag,$alarm_interval,$monitor_name,$monitor_type,$threshold,$sub_monitor_value,$alarm_info,$alarm_times);
                                                                     alarm_info_to_db($date,$tflag,$alarm_interval,$monitor_name,$monitor_type,$threshold,$sub_monitor_value,$alarm_info,$alarm_times);
                                                             }
                                                     }
                                             }
                                     }
                             }
                     }
             }
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

        my $table_prefix  = `date -d "10 minutes ago" +"%Y%m%d"` ; chomp $table_prefix;
        my $table_name = "t_imdata_taf_alarm_info_stat"."_". $table_prefix;
        my $stat_name = "taf_alarm_info";

        my $create_table_sql = qq{
        create table if not exists  $table_name  (
               `f_date` date NOT NULL default '1970-01-01',
               `f_tflag` varchar(8) NOT NULL default '',
               `f_alarm_interval` varchar(32) NOT NULL default '',
               `f_mon` varchar(64) NOT NULL default '',
               `f_mon_type` varchar(64) NOT NULL default '',
               `f_threshold` varchar(32) NOT NULL default '',
               `f_sub_monitor_value`  varchar(64) NOT NULL default '',
               `f_alarm_info` varchar(128) NOT NULL default '0',
               `f_alarm_times` bigint(20) NOT NULL default '0',
               PRIMARY KEY  (`f_date`,`f_tflag`,`f_alarm_interval`,`f_mon`,`f_mon_type`,`f_threshold`,`f_sub_monitor_value`,`f_alarm_info`)
        ) ENGINE=MyISAM DEFAULT CHARSET=gbk
        };
        $dbh1->do($create_table_sql);

        my ($date,$tflag,$alarm_interval,$monitor_name,$monitor_type,$threshold,$sub_monitor_value,$alarm_info,$alarm_times) = @_;
		my $stat_tm = `date -d "now" +"%H:%M"`; chomp $stat_tm;
        if ($stat_tm =~ /(\d+):(\d+)/) {
			$stat_tm = sprintf("%02d%02d", $1,  ceil(($2/10.0) + 0.05) * 10);
        }
        if ( $stat_tm =~/^00/) {
			$date = `date -d "now" +"%F"`;chomp($date);
		}
            
        printf("%s%-8s%-8s%-16s%-16s%-16s%-8s%-16s%-16s%-8d\n",$table_name,$date,$tflag,$alarm_interval,$monitor_name,$monitor_type,$threshold,$sub_monitor_value,$alarm_info,$alarm_times);
        my $sql_data3  = qq { INSERT INTO $table_name 
                             (f_date, f_tflag,f_alarm_interval, f_mon,f_mon_type,f_threshold,f_sub_monitor_value,f_alarm_info,f_alarm_times)  
                             VALUES (?,?,?,?,?,?,?,?,?) };
        my $sth_data3 = $dbh1->prepare($sql_data3);
        $sth_data3->execute($date,$tflag,$alarm_interval,$monitor_name,$monitor_type,$threshold,$sub_monitor_value,$alarm_info,$alarm_times);

        my $sql_ecstatus1 = qq { replace into t_ecstatus (appname,checkint,lasttime) values(?, ?, ?) };
        my $sth_ecstatus1 = $dbh1->prepare($sql_ecstatus1);

        $date =~ s/\-//g;
        $sth_ecstatus1->execute("$stat_name", 10, "$date $tflag" );


}
sub format_time_to_standard #将统计时间转换为1小时后的标准时间
{
        my $d = shift @_;
        my ($date,$tm) = split(/\s+/,$d);

        my $hour = substr($tm,0,2);
        my $min = substr($tm,2,2) ;
        if ($tm =~/60/) {
                $hour = substr($tm,0,2) +1;
                $min = substr($tm,2,2) ;
        }
        if ($hour == 24) {$hour = "00";$date=`date -d "$date 1 day" +"%F"`;chomp($date);}
        if ($min == 60) {$min ="00";}
        $tm = "$hour".":"."$min";
        my $time_end = `date -d "$date $tm 1 hour" +%F_%H%M`;chomp($time_end);

        return $time_end;
}
sub format_time_to_stat  #将标准时间转化为统计时间
{
        my $d = shift @_;
        my ($date,$tm) = split(/_/,$d);

        #printf("$date $tm\n");
        my $hour = substr($tm,0,2);
        my $min = substr($tm,2,2) ;
        if ($tm =~/00$/) {
                $hour = substr($tm,0,2) -1;
                if ($hour <  0) {$hour = "23";$date=`date -d "$date 1 day ago" +"%F"`;chomp($date);}
                if ($hour < 10 ) {$hour = "0"."$hour";}
                $min = substr($tm,2,2) ;
        }
        if ($min == 00) {$min ="60";}
        $tm = "$hour"."$min";
        return "$date $tm\n";
}

##################################################################################################################
#################################             main                       #########################################
##################################################################################################################
init;
get_statistic_data;
statistic_data_to_db;
alarm_info;

__DATA__