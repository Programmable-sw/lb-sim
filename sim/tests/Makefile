LIBS=-L.. -lhtsim
INCLUDE= -I../ -I./
LIBDEP=../libhtsim.a
.DEFAULT_GOAL := all
htsim_sglb_score_topk: main_sglb_score_topk.cpp ../datacenter/sglb_score_topk.h
	g++ -std=c++11 -Wall -O2 main_sglb_score_topk.cpp -o htsim_sglb_score_topk

CC=g++
CFLAGS = -Wall -std=c++11 -g -Wsign-compare
#CFLAGS += -fsanitize=address -fno-omit-frame-pointer -fsanitize=undefined
CFLAGS += -O2
SGLB_DATACENTER_OBJ_NAMES=firstfit.o vl2_topology.o fat_tree_topology.o bcube_topology.o connection_matrix.o oversubscribed_fat_tree_topology.o shortflows.o multihomed_fat_tree_topology.o star_topology.o fat_tree_switch.o
SGLB_DATACENTER_OBJS=$(addprefix ../datacenter/,$(SGLB_DATACENTER_OBJ_NAMES))

all:	htsim_broadcom_mmu htsim_conga_model htsim_dumbell_ndp htsim_dumbell_ndptunnel htsim_dumbell_tcp htsim_dumbell_swift htsim_multihop_swift htsim_multihop_swift2 htsim_bidir_swift htsim_bidir_ndp htsim_multipath_swift htsim_trigger_test htsim_dumbell_roce htsim_roce_gbn_ooo htsim_roce_sack_fields htsim_roce_sp_sack_correctness htsim_roce_exact_bounded_recovery htsim_roce_schedule htsim_roce_reps_warmup htsim_roce_reps_mrc_semantics htsim_roce_mrc_skip_semantics htsim_hybrid_nmrc htsim_sglb_quality htsim_sglb_gcn htsim_paper_sglb htsim_nmrc_score htsim_nmrc_shuffled_bucket htsim_stor_feedback htsim_stor_score_trace htsim_composite_ecn htsim_lossless_ecn_counter htsim_connection_matrix_lb htsim_dumbell_hpcc htsim_dumbell_strack

htsim_broadcom_mmu: main_broadcom_mmu.cpp ../broadcom_mmu.h
	$(CC) $(CFLAGS) main_broadcom_mmu.cpp -o htsim_broadcom_mmu

htsim_conga_model: main_conga_model.cpp ../conga_model.h
	$(CC) $(CFLAGS) main_conga_model.cpp -o htsim_conga_model

htsim_connection_matrix_lb:	main_connection_matrix_lb.o $(LIBDEP) ../datacenter/connection_matrix.o
	$(CC) $(CFLAGS) main_connection_matrix_lb.o ../datacenter/connection_matrix.o -o htsim_connection_matrix_lb $(LIBS)

main_connection_matrix_lb.o:	main_connection_matrix_lb.cpp ../datacenter/connection_matrix.h

htsim_dumbell_ndp:  main_dumbell_ndp.o $(LIBDEP)
	$(CC) $(CFLAGS) main_dumbell_ndp.o -o htsim_dumbell_ndp $(LIBS)

htsim_dumbell_roce:	main_dumbell_roce.o $(LIBDEP)
	$(CC) $(CFLAGS) main_dumbell_roce.o -o htsim_dumbell_roce $(LIBS)

htsim_roce_gbn_ooo:	main_roce_gbn_ooo.o $(LIBDEP)
	$(CC) $(CFLAGS) main_roce_gbn_ooo.o -o htsim_roce_gbn_ooo $(LIBS)

htsim_roce_sack_fields:	main_roce_sack_fields.o $(LIBDEP)
	$(CC) $(CFLAGS) main_roce_sack_fields.o -o htsim_roce_sack_fields $(LIBS)

htsim_roce_sp_sack_correctness:	main_roce_sp_sack_correctness.o $(LIBDEP)
	$(CC) $(CFLAGS) main_roce_sp_sack_correctness.o -o htsim_roce_sp_sack_correctness $(LIBS)

main_roce_sp_sack_correctness.o: main_roce_sp_sack_correctness.cpp ../roce.h ../rocepacket.h

htsim_roce_exact_bounded_recovery:	main_roce_exact_bounded_recovery.o $(LIBDEP)
	$(CC) $(CFLAGS) main_roce_exact_bounded_recovery.o -o htsim_roce_exact_bounded_recovery $(LIBS)

main_roce_exact_bounded_recovery.o: main_roce_exact_bounded_recovery.cpp ../roce.h ../rocepacket.h

htsim_roce_schedule:	main_roce_schedule.o $(LIBDEP)
	$(CC) $(CFLAGS) main_roce_schedule.o -o htsim_roce_schedule $(LIBS)

htsim_roce_reps_warmup:	main_roce_reps_warmup.o $(LIBDEP)
	$(CC) $(CFLAGS) main_roce_reps_warmup.o -o htsim_roce_reps_warmup $(LIBS)

htsim_roce_reps_mrc_semantics:	main_roce_reps_mrc_semantics.o $(LIBDEP)
	$(CC) $(CFLAGS) main_roce_reps_mrc_semantics.o -o htsim_roce_reps_mrc_semantics $(LIBS)

main_roce_reps_mrc_semantics.o: main_roce_reps_mrc_semantics.cpp ../roce.h ../rocepacket.h

htsim_roce_mrc_skip_semantics:	main_roce_mrc_skip_semantics.o $(LIBDEP)
	$(CC) $(CFLAGS) main_roce_mrc_skip_semantics.o -o htsim_roce_mrc_skip_semantics $(LIBS)

main_roce_mrc_skip_semantics.o: main_roce_mrc_skip_semantics.cpp ../roce.h ../rocepacket.h

htsim_hybrid_nmrc:	main_hybrid_nmrc.o $(LIBDEP)
	$(CC) $(CFLAGS) main_hybrid_nmrc.o -o htsim_hybrid_nmrc $(LIBS)

main_hybrid_nmrc.o: main_hybrid_nmrc.cpp ../roce.h ../rocepacket.h

.PHONY: sglb_datacenter_objects
sglb_datacenter_objects:	$(LIBDEP)
	$(MAKE) -C ../datacenter $(SGLB_DATACENTER_OBJ_NAMES)

htsim_sglb_quality:	main_sglb_quality.o $(LIBDEP) sglb_datacenter_objects
	$(CC) $(CFLAGS) main_sglb_quality.o $(SGLB_DATACENTER_OBJS) -o htsim_sglb_quality $(LIBS)

main_sglb_quality.o:	main_sglb_quality.cpp ../datacenter/fat_tree_switch.h

htsim_sglb_gcn:	main_sglb_gcn.o $(LIBDEP) sglb_datacenter_objects
	$(CC) $(CFLAGS) main_sglb_gcn.o $(SGLB_DATACENTER_OBJS) -o htsim_sglb_gcn $(LIBS)

main_sglb_gcn.o:	main_sglb_gcn.cpp ../datacenter/fat_tree_switch.h ../sglbpacket.h

htsim_paper_sglb:	main_paper_sglb.o $(LIBDEP) sglb_datacenter_objects
	$(CC) $(CFLAGS) main_paper_sglb.o $(SGLB_DATACENTER_OBJS) -o htsim_paper_sglb $(LIBS)

main_paper_sglb.o:	main_paper_sglb.cpp ../datacenter/fat_tree_switch.h ../sglbpacket.h

htsim_nmrc_score:	main_nmrc_score.o $(LIBDEP) sglb_datacenter_objects
	$(CC) $(CFLAGS) main_nmrc_score.o $(SGLB_DATACENTER_OBJS) -o htsim_nmrc_score $(LIBS)

main_nmrc_score.o:	main_nmrc_score.cpp ../datacenter/fat_tree_switch.h

htsim_nmrc_shuffled_bucket:	main_nmrc_shuffled_bucket.o $(LIBDEP)
	$(CC) $(CFLAGS) main_nmrc_shuffled_bucket.o -o htsim_nmrc_shuffled_bucket $(LIBS)

main_nmrc_shuffled_bucket.o:	main_nmrc_shuffled_bucket.cpp ../roce.h

htsim_stor_feedback:	main_stor_feedback.o $(LIBDEP) sglb_datacenter_objects
	$(CC) $(CFLAGS) main_stor_feedback.o $(SGLB_DATACENTER_OBJS) -o htsim_stor_feedback $(LIBS)

main_stor_feedback.o:	main_stor_feedback.cpp ../datacenter/fat_tree_switch.h ../roce.h ../rocepacket.h

htsim_stor_score_trace:	main_stor_score_trace.o $(LIBDEP) sglb_datacenter_objects
	$(CC) $(CFLAGS) main_stor_score_trace.o $(SGLB_DATACENTER_OBJS) -o htsim_stor_score_trace $(LIBS)

main_stor_score_trace.o:	main_stor_score_trace.cpp ../datacenter/fat_tree_switch.h

htsim_composite_ecn:	main_composite_ecn.o $(LIBDEP) sglb_datacenter_objects
	$(CC) $(CFLAGS) main_composite_ecn.o $(SGLB_DATACENTER_OBJS) -o htsim_composite_ecn $(LIBS)

main_composite_ecn.o:	main_composite_ecn.cpp ../datacenter/fat_tree_topology.h ../compositequeue.h

htsim_lossless_ecn_counter:	main_lossless_ecn_counter.o $(LIBDEP)
	$(CC) $(CFLAGS) main_lossless_ecn_counter.o -o htsim_lossless_ecn_counter $(LIBS)

htsim_dumbell_hpcc:	main_dumbell_hpcc.o $(LIBDEP)
	$(CC) $(CFLAGS) main_dumbell_hpcc.o -o htsim_dumbell_hpcc $(LIBS)

htsim_dumbell_tcp:	main_dumbell_tcp.o $(LIBDEP)
	$(CC) $(CFLAGS) main_dumbell_tcp.o -o htsim_dumbell_tcp $(LIBS)

htsim_dumbell_swift:	main_dumbell_swift.o $(LIBDEP)
	$(CC) $(CFLAGS) main_dumbell_swift.o -o htsim_dumbell_swift $(LIBS)

htsim_multihop_swift:	main_multihop_swift.o $(LIBDEP)
	$(CC) $(CFLAGS) main_multihop_swift.o -o htsim_multihop_swift $(LIBS)

htsim_multihop_swift2:	main_multihop_swift2.o $(LIBDEP)
	$(CC) $(CFLAGS) main_multihop_swift2.o -o htsim_multihop_swift2 $(LIBS)

htsim_multipath_swift:	main_multipath_swift.o $(LIBDEP)
	$(CC) $(CFLAGS) main_multipath_swift.o -o htsim_multipath_swift $(LIBS)

htsim_bidir_swift:	main_bidir_swift.o $(LIBDEP)
	$(CC) $(CFLAGS) main_bidir_swift.o -o htsim_bidir_swift $(LIBS)

htsim_mpswift:	main_mpswift.o $(LIBDEP)
	$(CC) $(CFLAGS) main_mpswift.o -o htsim_mpswift $(LIBS)

htsim_dumbell_strack:	main_dumbell_strack.o $(LIBDEP)
	$(CC) $(CFLAGS) main_dumbell_strack.o -o htsim_dumbell_strack $(LIBS)

htsim_bidir_ndp:	main_bidir_ndp.o $(LIBDEP)
	$(CC) $(CFLAGS) main_bidir_ndp.o -o htsim_bidir_ndp $(LIBS)

htsim_dumbell_ndptunnel:	main_dumbell_ndptunnel.o $(LIBDEP)
	$(CC) $(CFLAGS) main_dumbell_ndptunnel.o -o htsim_dumbell_ndptunnel $(LIBS)

htsim_trigger_test:	main_trigger_test.o $(LIBDEP)
	$(CC) $(CFLAGS) main_trigger_test.o -o htsim_trigger_test $(LIBS)

RUN_TESTS_0 = ./tests.py
RUN_TESTS_1 = ./tests.py -v
V ?= 0

.PHONY: run
run: all
	$(RUN_TESTS_$(V))

.PHONY: update_refs
update_refs: all
	$(RUN_TESTS_$(V)) -u


clean:	
	rm -f *.o htsim_*

.cpp.o:
	source='$<' object='$@' libtool=no depfile='$(DEPDIR)/$*.Po' tmpdepfile='$(DEPDIR)/$*.TPo' $(CXXDEPMODE) $(depcomp) $(CC) $(CFLAGS) $(INCLUDE) -Wuninitialized  -c -o $@ `test -f $< || echo '$(srcdir)/'`$<
