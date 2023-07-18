package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;
import java.util.List;

@SpringBootTest
@Transactional
public class QueryDslBasicTest {

    public static final QMember MEMBER = QMember.member;
    public static final QTeam TEAM = QTeam.team;
    @PersistenceContext
    EntityManager em;
    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        // member1을 찾아라!
        Member findByMember = em.createQuery(
                        "select m from Member m " +
                                "where m.username = :username"
                        , Member.class
                ).setParameter("username", "member1")
                .getSingleResult();

        Assertions.assertThat(findByMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQueryDsl() {
        QMember m = new QMember("m");                // 별칭을 선언

        Member findByMember = queryFactory                  // SQL 선언
                .select(m)
                .from(m)
                .where(m.username.eq("member1"))       // 자동으로 Preparedstatement를 사용하여 기능 제공
                .fetchOne();

        Assertions.assertThat(findByMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findByMember = queryFactory
                .selectFrom(MEMBER)
                .where(
                        MEMBER.username.eq("member1")
                                .and(MEMBER.age.eq(10))
                )
                .fetchOne();

        Assertions.assertThat(findByMember.getUsername()).isEqualTo("member1");
        Assertions.assertThat(findByMember.getAge()).isEqualTo(10);
    }

    /**
     * QueryDsl의 where 절에서 조건을 And 대신 ,(쉼표)를 사용하여 연결해도 QueryDsl 내에서 And 처리가 된다.
     * 다만, ,(쉼표)는 null인 경우는 무시한다.
     */
    @Test
    public void searchAndParam() {
        Member findByMember = queryFactory
                .selectFrom(MEMBER)
                .where(
                        MEMBER.username.eq("member1")
                        , (MEMBER.age.eq(10))
                )
                .fetchOne();

        Assertions.assertThat(findByMember.getUsername()).isEqualTo("member1");
        Assertions.assertThat(findByMember.getAge()).isEqualTo(10);
    }

    @Test
    public void resultFetch() {
        /*
        List<Member> fetch = queryFactory
                .selectFrom(MEMBER)
                .fetch();

        Member fetchOne = queryFactory
                .selectFrom(MEMBER)
                .fetchOne();

        Member fetchFirst = queryFactory
                .selectFrom(MEMBER)
                .fetchFirst();
        */

        QueryResults<Member> results = queryFactory
                .selectFrom(MEMBER)
                .fetchResults();

        results.getTotal();
        List<Member> content = results.getResults();

        long total = queryFactory
                .selectFrom(MEMBER)
                .fetchCount();
    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(DESC)
     * 2. 회원 이름 올림차순(ASC)
     * <p>
     * 단, 2에서 회원 이름이 없으면 마지막 출력 (Nulls last)
     */
    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(MEMBER)
                .where(MEMBER.age.eq(100))
                .orderBy(MEMBER.age.desc(), MEMBER.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);
        Assertions.assertThat(member5.getUsername()).isEqualTo("member5");
        Assertions.assertThat(member6.getUsername()).isEqualTo("member6");
        Assertions.assertThat(memberNull.getUsername()).isNull();
    }

    /**
     * QueryDsl 페이징 처리
     */
    @Test
    public void paging1() {
        List<Member> result = queryFactory
                .selectFrom(MEMBER)
                .orderBy(MEMBER.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        Assertions.assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2() {
        QueryResults<Member> queryResults = queryFactory
                .selectFrom(MEMBER)
                .orderBy(MEMBER.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        Assertions.assertThat(queryResults.getTotal()).isEqualTo(4);
        Assertions.assertThat(queryResults.getLimit()).isEqualTo(2);
        Assertions.assertThat(queryResults.getOffset()).isEqualTo(1);
        Assertions.assertThat(queryResults.getResults().size()).isEqualTo(2);
    }

    /**
     * 집합
     */
    @Test
    public void aggregation() {
        List<Tuple> result = queryFactory
                .select(
                        MEMBER.count()
                        , MEMBER.age.sum()
                        , MEMBER.age.avg()
                        , MEMBER.age.max()
                        , MEMBER.age.min()
                )
                .from(MEMBER)
                .fetch();

        Tuple tuple = result.get(0);
        Assertions.assertThat(tuple.get(MEMBER.count())).isEqualTo(4);
        Assertions.assertThat(tuple.get(MEMBER.age.sum())).isEqualTo(100);
        Assertions.assertThat(tuple.get(MEMBER.age.avg())).isEqualTo(25);
        Assertions.assertThat(tuple.get(MEMBER.age.max())).isEqualTo(40);
        Assertions.assertThat(tuple.get(MEMBER.age.min())).isEqualTo(10);
    }

    /**
     * 팀 이름과 각 팀의 평균 연령을 구해라.
     */
    @Test
    public void group() throws Exception {
        List<Tuple> result = queryFactory
                .select(TEAM.name, MEMBER.age.avg())
                .from(MEMBER)
                .join(MEMBER.team, TEAM)
                .groupBy(TEAM.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        Assertions.assertThat(teamA.get(TEAM.name)).isEqualTo("teamA");
        Assertions.assertThat(teamA.get(MEMBER.age.avg())).isEqualTo(15);

        Assertions.assertThat(teamB.get(TEAM.name)).isEqualTo("teamB");
        Assertions.assertThat(teamB.get(MEMBER.age.avg())).isEqualTo(35);
    }

    /**
     * QueryDsl Join
     * - Team A에 소속된 모든 회원 조회
     */
    @Test
    public void join() {
        List<Member> result = queryFactory
                .selectFrom(MEMBER)
                .join(MEMBER.team, TEAM)
                .where(TEAM.name.eq("teamA"))
                .fetch();

        Assertions.assertThat(result).extracting("username").containsExactly("member1", "member2");
    }

    /**
     * 세타 조인
     * - 회원의 이름이 팀 이름과 동일한 회원 조인 조회
     * <p>
     * 동작 방식
     * - 모든 정보를 가져와서 조인 후 WHERE에서 필터를 진행
     */
    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Member> result = queryFactory
                .select(MEMBER)
                .from(MEMBER, TEAM)
                .where(MEMBER.username.eq(TEAM.name))
                .fetch();

        Assertions.assertThat(result).extracting("username").containsExactly("teamA", "teamB");
    }

    /**
     * Join On을 사용한 조인 (버전 2.1 부터 지원)
     * - 회원과 팀을 조인하면서, 팀 이름이 TeamA인 팀만 조회, 회원은 모두 조회
     * JPQL : select m, t from Member m left join m.team t on t.name = 'teamA';
     */
    @Test
    public void join_on_filtering() {
        List<Tuple> result = queryFactory
                .select(MEMBER, TEAM)
                .from(MEMBER)
                .leftJoin(MEMBER.team, TEAM).on(TEAM.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 연관관계가 없는 Entity 외부 조인
     * - 회원의 이름이 팀 이름과 동일한 대상 외부 조인
     */
    @Test
    public void join_on_no_relation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(MEMBER, TEAM)
                .from(MEMBER)
                .leftJoin(TEAM).on(MEMBER.username.eq(TEAM.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * Fetch Join
     */
    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() {
        em.flush();
        em.clear();

        Member findByMember = queryFactory
                .selectFrom(MEMBER)
                .where(MEMBER.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findByMember.getTeam());
        Assertions.assertThat(loaded).as("페치 조인 미적용").isFalse();
    }

    @Test
    public void fetchJoinUse() {
        em.flush();
        em.clear();

        Member findByMember = queryFactory
                .selectFrom(MEMBER)
                .join(MEMBER.team, TEAM).fetchJoin()
                .where(MEMBER.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findByMember.getTeam());
        Assertions.assertThat(loaded).as("페치 조인 미적용").isTrue();
    }

    /**
     * QueryDsl 을 사용한 SubQuery
     * - 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(MEMBER)
                .where(
                        MEMBER.age.eq(
                                JPAExpressions
                                        .select(memberSub.age.max())
                                        .from(memberSub)
                        )
                )
                .fetch();

        Assertions.assertThat(result).extracting("age").containsExactly(40);
    }

    /**
     * QueryDsl 을 사용한 SubQuery
     * - 나이가 평균 이상인 회원
     */
    @Test
    public void subQueryGoe() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(MEMBER)
                .where(
                        MEMBER.age.goe(
                                JPAExpressions
                                        .select(memberSub.age.avg())
                                        .from(memberSub)
                        )
                )
                .fetch();

        Assertions.assertThat(result).extracting("age").containsExactly(30, 40);
    }

    /**
     * QueryDsl 을 사용한 SubQuery
     * - Select 내에 SubQuery 사용
     */
    @Test
    public void selectSubQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(MEMBER.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                )
                .from(MEMBER)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * Case 문
     */
    @Test
    public void basicCase() {
        List<String> result = queryFactory
                .select(MEMBER.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(MEMBER)
                .fetch();

        for (String s : result) {
            System.out.println("S = " + s);
        }
    }
    
    @Test
    public void complexCase() {
        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(MEMBER.age.between(0, 20)).then("0~20살")
                        .when(MEMBER.age.between(21, 30)).then("21~30살")
                        .otherwise("기타"))
                .from(MEMBER)
                .fetch();

        for (String s : result) {
            System.out.println("S = " + s);
        }
    }

    /**
     * 상수 및 문자 concat 처리
     */
    @Test
    public void constant() {
        List<Tuple> result = queryFactory
                .select(MEMBER.username, Expressions.constant("A"))
                .from(MEMBER)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void concat() {
        // {username}_{age}
        List<String> result = queryFactory
                .select(MEMBER.username.concat("_").concat(MEMBER.age.stringValue()))
                .from(MEMBER)
                .where(MEMBER.username.eq("member1"))
                .fetch();

        for (String s : result) {
            System.out.println("S = " + s);
        }
    }

    /**
     * Tuple 사용
     */
    @Test
    public void simpleProjection() {
        List<String> result = queryFactory
                .select(MEMBER.username)
                .from(MEMBER)
                .fetch();

        for (String s : result) {
            System.out.println("S = " + s);
        }
    }

    @Test
    public void tupleProjection() {
        List<Tuple> result = queryFactory
                .select(MEMBER.username, MEMBER.age)
                .from(MEMBER)
                .fetch();

        for (Tuple tuple : result) {
            String username = tuple.get(MEMBER.username);
            Integer age = tuple.get(MEMBER.age);

            System.out.println("age = " + age);
        }
    }

    /**
     * 순수 JPA에서 DTO로 조회
     */
    @Test
    public void findDtoByJPQL() {
        List<MemberDto> result = em.createQuery(
                "select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m"
                , MemberDto.class
        ).getResultList();

        for(MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * Bean 형식은 Setter를 통해서 데이터를 저장
     */
    @Test
    public void findDtoBySetter() {
        List<MemberDto> result = queryFactory
                .select(
                        Projections.bean(MemberDto.class, MEMBER.username, MEMBER.age)
                ).from(MEMBER)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * Field 형식은 Setter를 사용하지 않고, 필드에 바로 데이터 저장
     */
    @Test
    public void findDtoByField() {
        List<MemberDto> result = queryFactory
                .select(
                        Projections.fields(MemberDto.class, MEMBER.username, MEMBER.age)
                ).from(MEMBER)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * constructor(생성자) 형식은 저장할 데이터를 파라미터로 받을 수 있는 생성자의 존재가 필요
     */
    @Test
    public void findDtoByConstructor() {
        List<MemberDto> result = queryFactory
                .select(
                        Projections.constructor(MemberDto.class, MEMBER.username, MEMBER.age)
                ).from(MEMBER)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void findUserDto() {
        QMember memberSub = new QMember("memberSub");

        List<UserDto> result = queryFactory
                .select(
                        //Projections.constructor(UserDto.class, MEMBER.username.as("name"), MEMBER.age)
                        Projections.fields(
                                UserDto.class
                                , MEMBER.username.as("name")
                                , ExpressionUtils.as(JPAExpressions
                                        .select(memberSub.age.max())
                                        .from(memberSub)
                                        , "age"
                                ))
                ).from(MEMBER)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("memberDto = " + userDto);
        }
    }

    /**
     * 프로젝션과 결과 반환 - `@QueryProjection`
     */
    @Test
    public void findDtoByQueryProjection() {
        List<MemberDto> result = queryFactory
                .select(new QMemberDto(MEMBER.username, MEMBER.age))
                .from(MEMBER)
                .fetch();

        for(MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * 동적 쿼리 적용
     * - BooleanBuilder 사용
     */
    @Test
    public void dynamicQuery_BooleanBuilder() {
        String usernameParam = "member1";
        Integer ageParam = null;

        List<Member> result = searchMember1(usernameParam, ageParam);
        Assertions.assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {
        BooleanBuilder builder = new BooleanBuilder();

        if (usernameCond != null) {
            builder.and(MEMBER.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(MEMBER.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(MEMBER)
                .where(builder)
                .fetch();
    }

    /**
     * 동적 쿼리 적용
     * - Where 사용
     */
    @Test
    public void dynamicQuery_WhereParam() {
        String usernameParam = "member1";
        Integer ageParam = null;

        List<Member> result = searchMember2(usernameParam, ageParam);
        Assertions.assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return queryFactory
                .selectFrom(MEMBER)
                .where(usernameEq(usernameCond), ageEq(ageCond))
                .fetch();
    }

    private Predicate usernameEq(String usernameCond) {
        if(usernameCond == null) {
            return null;
        }

        return MEMBER.username.eq(usernameCond);
        // return usernameCond == null ? null : MEMBER.username.eq(usernameCond);
    }

    private Predicate ageEq(Integer ageCond) {
        if(ageCond == null) {
            return null;
        }

        return MEMBER.age.eq(ageCond);
        // return ageCond == null ? null : MEMBER.age.eq(ageCond);
    }

    /**
     * 동적 쿼리 적용
     * - Where + BooleanBuilder 사용
     */
    @Test
    public void dynamicQuery_WhereAndBooleanBuilderParam() {
        String usernameParam = "member1";
        Integer ageParam = null;

        List<Member> result = searchMember3(usernameParam, ageParam);
        Assertions.assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember3(String usernameCond, Integer ageCond) {
        return queryFactory
                .selectFrom(MEMBER)
                .where(allEq(usernameCond, ageCond))
                .fetch();
    }

    private BooleanExpression usernameEq1(String usernameCond) {
        return usernameCond != null ? MEMBER.username.eq(usernameCond) : null;
    }

    private BooleanExpression ageEq1(Integer ageCond) {
        return ageCond != null ? MEMBER.age.eq(ageCond) : null;
    }

    private BooleanExpression allEq(String usernameCond, Integer ageCond) {
        return usernameEq1(usernameCond).and(ageEq1(ageCond));
    }

    /**
     * 벌크 연산
     */
    @Test
    public void bulkUpdate() {
        /*
         * 실행되기 전
         *  member1 = 10 -> DB member1      |      Context member1
         *  member2 = 20 -> DB member2      |      Context member2
         *  member3 = 30 -> DB member3      |      Context member3
         *  member4 = 40 -> DB member4      |      Context member4
         */

        long count = queryFactory
                .update(MEMBER)
                .set(MEMBER.username, "비회원")
                .where(MEMBER.age.lt(28))
                .execute();

        /*
         * 실행 후
         *  member1 = 10 -> DB 비회원        |      Context member1
         *  member2 = 20 -> DB 비회원        |      Context member2
         *  member3 = 30 -> DB member3      |      Context member3
         *  member4 = 40 -> DB member4      |      Context member4
         */

        em.flush();
        em.clear();

        List<Member> result = queryFactory
                .selectFrom(MEMBER)
                .fetch();

        /*
         * 영속성 컨텍스트 후 조회 실행 후
         *  member1 = 10 -> DB 비회원        |      Context 비회원
         *  member2 = 20 -> DB 비회원        |      Context 비회원
         *  member3 = 30 -> DB member3      |      Context member3
         *  member4 = 40 -> DB member4      |      Context member4
         */

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);

            /*
             * em.flush() / em.clear() 적용 X
             *   member1 = Member(id=3, username=member1, age=10)
             *   member1 = Member(id=4, username=member2, age=20)
             *   member1 = Member(id=5, username=member3, age=30)
             *   member1 = Member(id=6, username=member4, age=40)
             *
             * em.flush() / em.clear() 적용 O
             *   member1 = Member(id=3, username=비회원, age=10)
             *   member1 = Member(id=4, username=비회원, age=20)
             *   member1 = Member(id=5, username=member3, age=30)
             *   member1 = Member(id=6, username=member4, age=40)
             */
        }
    }

    @Test
    public void bulkAdd() {
        long count = queryFactory
                .update(MEMBER)
                .set(MEMBER.age, MEMBER.age.add(1))
                .execute();
    }

    @Test
    public void bulkDelete() {
        long count = queryFactory
                .delete(MEMBER)
                .where(MEMBER.age.gt(18))
                .execute();
    }

    /**
     * SQL Function 호출
     */
    @Test
    public void sqlFunction() {
        List<String> result = queryFactory
                .select(Expressions.stringTemplate(
                        "function('replace', {0}, {1}, {2})"
                        , MEMBER.username
                        , "member"
                        , "M"
                )).from(MEMBER)
                .fetch();
        
        for (String s : result) {
            System.out.println("S = " + s);
        }
    }

    @Test
    public void sqlFunction2() {
        List<String> result = queryFactory
                .select(MEMBER.username)
                .from(MEMBER)
//                .where(MEMBER.username.eq(
//                        Expressions.stringTemplate("function('lower', {0})", MEMBER.username)
//                ))
                .where(MEMBER.username.eq(MEMBER.username.lower()))         // 기본적으로 안시 표준으로 모든 데이터베이스에서 제공되는 기능은 함수형태로 생략하여 작성 가능
                .fetch();

        for (String s : result) {
            System.out.println("S = " + s);
        }
    }
}
