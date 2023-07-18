package study.querydsl.repository.support;

import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQuery;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.util.StringUtils;
import study.querydsl.dto.MemberSearchCondition;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.repository.support.Querydsl4RepositorySupport;

import java.util.List;

public class MemberTestRepository extends Querydsl4RepositorySupport {

    public MemberTestRepository() {
        super(Member.class);
    }

    public List<Member> basicSelect() {
        return select(QMember.member)
                .from(QMember.member)
                .fetch();
    }

    public List<Member> basicSelectFrom() {
        return selectFrom(QMember.member)
                .fetch();
    }

    /**
     * QueryDsl의 기본 기능으로 작성
     */
    public Page<Member> searchPageByApplyPage(MemberSearchCondition condition, Pageable pageable) {
        JPAQuery<Member> query = selectFrom(QMember.member)
                .where(
                        usernameEq(condition.getUsername())
                        ,teamNameEq(condition.getTeamName())
                        , ageGoe(condition.getAgeGoe())
                        , ageLoe(condition.getAgeLoe())
                );

        List<Member> content = getQuerydsl().applyPagination(pageable, query).fetch();
        return PageableExecutionUtils.getPage(content, pageable, query::fetchCount);
    }

    /**
     * QueryDsl의 기능을 커스텀하여 작성
     */
    public Page<Member> applyPagination(MemberSearchCondition condition, Pageable pageable) {
        return applyPagination(pageable, query ->
            query.selectFrom(QMember.member)
                    .leftJoin(QMember.member.team, QTeam.team)
                    .where(
                            usernameEq(condition.getUsername())
                            ,teamNameEq(condition.getTeamName())
                            , ageGoe(condition.getAgeGoe())
                            , ageLoe(condition.getAgeLoe())
                    )
        );
    }

    public Page<Member> applyPagination2(MemberSearchCondition condition, Pageable pageable) {
        return applyPagination(pageable, contentQuery ->
                // Content Query
                contentQuery.selectFrom(QMember.member)
                        .leftJoin(QMember.member.team, QTeam.team)
                        .where(
                                usernameEq(condition.getUsername())
                                ,teamNameEq(condition.getTeamName())
                                , ageGoe(condition.getAgeGoe())
                                , ageLoe(condition.getAgeLoe())
                        )

                // Count Query
                , countQuery ->
                        countQuery.select(QMember.member.id)
                                .from(QMember.member)
                                .leftJoin(QMember.member.team, QTeam.team)
                                .where(
                                        usernameEq(condition.getUsername())
                                        , teamNameEq(condition.getTeamName())
                                        , ageGoe(condition.getAgeGoe())
                                        , ageLoe(condition.getAgeLoe())
                                )
        );
    }

    private BooleanExpression usernameEq(String username) {
        return StringUtils.hasText(username) ? QMember.member.username.eq(username) : null;
    }

    private BooleanExpression teamNameEq(String teamName) {
        return StringUtils.hasText(teamName) ? QTeam.team.name.eq(teamName) : null;
    }

    private BooleanExpression ageGoe(Integer ageGoe) {
        return ageGoe != null ? QMember.member.age.goe(ageGoe) : null;
    }

    private BooleanExpression ageLoe(Integer ageLoe) {
        return ageLoe != null ? QMember.member.age.goe(ageLoe) : null;
    }
}
